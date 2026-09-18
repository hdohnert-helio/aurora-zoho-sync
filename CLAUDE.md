# aurora-zoho-sync — working notes

FastAPI service (deployed on Render at `aurora-zoho-sync.onrender.com`) plus
GitHub Actions jobs that sync Aurora Solar, Zoho CRM, Google and Twilio.
Owner: Harry Dohnert (COO, Helio Solar).

## Current task: PowerClerk interconnection scraper

**Goal.** Read UI and Eversource interconnection status from PowerClerk on a
schedule, write it to new Zoho fields, and raise a task + SMS when a project
needs Helio's action. Emails alone are insufficient — they say "log in to see
the response", and the correction detail only exists in the portal.

### What is already proven
- `scripts/powerclerk_smoke.py` — PASSES. Logs in and sees 196 UI + 312
  Eversource projects unattended.
- Credentials: 1Password vault `Helio Automation`, items `PowerClerk UI` /
  `PowerClerk Eversource`. GitHub secret `OP_SERVICE_ACCOUNT_TOKEN` holds a
  1Password **service account** token. Workflows load them via
  `1password/load-secrets-action@v2` into `PC_USER` / `PC_PASS`.
  **Never print or commit credential values.**

### PowerClerk facts (learned the hard way)
- Login: `https://{host}/MvcAccount/Login?ProgramId={pid}`, fields `#UserName`
  and `#Password`, submit by pressing Enter (the text "Log In" appears twice).
- **One session per account.** Logging in from Actions kicks Harry's browser
  out. A second PowerClerk user has been requested from the program admins;
  until it exists, run only when nobody is working in the portal.
- Session is **shared across programs** — log in once at `uinet`, then visit
  Eversource in the same context. Eversource has no public login page; hitting
  it signed-out redirects to `www.eversource.com`.
- Programs (same login): UI `uinet.powerclerk.com` / `MS7SMA7Q77CM`;
  Eversource `esctinterconnect.powerclerk.com` / `28X4MJXZUR42`;
  Con Ed `conedsmalldg.powerclerk.com` / `99Q1XX7VU6X8`;
  CT NRES `esctnresbid.powerclerk.com` / `DZJ3FHV4ZDK7`.
- Project list is a **Vue grid** (`data-v-` attributes), 13 rows/page, with no
  ordinary Next link. Do not try to click the pager.
- The grid calls `POST /MvcProjects/ProjectList/GetProjectList3` with a
  DataTables-style body:
  `{"programId":..., "tableRequest":{"start":0,"length":13,"columns":[{"header":"Project #","property":"ProjectData0",...}]}}`
  Set `length` high to get everything in one call.
- **Replaying that call from `page.request` fails** with
  `"Page session has timed out"` — there is a page-scoped token. Replay it from
  inside the page with `page.evaluate` + `fetch(..., {credentials:'same-origin'})`
  using the captured request headers. *(This is the step currently being fixed.)*
- Record page: `/MvcProjects/LandingPage?ProgramId=..&ProjectId=..` —
  `ProjectId` is an opaque token, NOT the DER/INT number, so build a map.
- Form page: `/MvcProjects/ViewProject?ProgramId=..&FormId=..&ProjectId=..`
- **Correction text lives in a read-only `<textarea>`** — read `.value`, not
  innerText. `get_page_text` returns it empty.
- Useful tab filters: UI "Application Corrections Required"; Eversource
  "Customer Action Required". These are the actionable lists.
- Columns worth capturing: Project #, Customer, Address, City, Current Status,
  Status Timestamp, plus **Queue Position** (UI) and **Assignee** (Eversource).
- Status vocabulary: UI `Energize System` and Eversource `Level 1 - Online`
  both mean PTO granted.

### Zoho side
- Module `Installs`. 49 projects are active (stage not Canceled/Project Closeout).
- `Utility_PTO` is null on **all** of them, including 11 at stage Energized.
- Only 17 of 49 carry `IC_Project_Number`, and INT-119881 is duplicated across
  both Alamgir projects. Portal shows **INT-119959 = 28-30 Aberdeen St**, so the
  28 Aberdeen record is the wrong one.
- Planned new fields (additive, do not overwrite `Utility_Status`):
  `IC_Portal_Status`, `IC_Portal_Status_At`, `IC_Portal_Action_Required`,
  `IC_Portal_Action_Detail`, `IC_Portal_Deadline`, `IC_Portal_Queue_Position`,
  `IC_Portal_Assignee`, `IC_Portal_URL`, `IC_Portal_Checked_At`.
- **All Zoho writes must send `"trigger": []`** so workflows, blueprints and
  assignment rules do not fire.

## Conventions
- Secrets live in GitHub Secrets / 1Password, never in the repo. `.gitignore`
  covers `client_secret*.json`, `*credentials*.json`, `*.gdoc`, `.env*`.
- Workflows run `python -u` so logs stream.
- Prefer a portal/vendor API over scraping whenever one exists. Harry is
  pursuing LightReach API access; PowerClerk API access is worth asking UI and
  Eversource for.

## Zoho credentials (1Password)

Vault `Helio Automation`, item `Zoho API` (API Credential type), three custom fields:

```
op://Helio Automation/Zoho API/ZOHO_CLIENT_ID
op://Helio Automation/Zoho API/ZOHO_CLIENT_SECRET
op://Helio Automation/Zoho API/ZOHO_REFRESH_TOKEN
```

Load them the same way `powerclerk-audit.yml` loads the PowerClerk creds:
`1password/load-secrets-action@v2` with `export-env: true` and
`OP_SERVICE_ACCOUNT_TOKEN: ${{ secrets.OP_SERVICE_ACCOUNT_TOKEN }}`.
**Never print or commit credential values.**

The refresh token does not expire on its own but is invalidated if the client
secret is regenerated or the connected app is revoked in Zoho. Auth failures
months from now are most likely this, not the scraper.

## Reconciliation state as of 2026-09-17

A one-time manual reconciliation of all 49 active Installs against 508 portal
projects is DONE. Do not redo it. What was written:

- 19 `IC_Project_Number` corrections/additions (incl. fixing INT-119881, which
  was duplicated across both Mohammed Alamgir records; 28 Aberdeen is INT-119959)
- 11 `Utility_PTO` dates, derived from portal status timestamps on
  `Energize System` (UI) / `Level 1 - Online` (Eversource)
- 45 records backfilled with the new `IC_Portal_*` fields

### New fields on Installs

| API name | Type | Meaning |
|---|---|---|
| `IC_Portal_Status` | text(255) | Verbatim portal status string |
| `IC_Portal_Status_Date` | date | The portal's own status timestamp |
| `IC_Action_Required` | boolean | Someone is blocking; see rule below |
| `IC_Portal_Checked` | datetime | Last successful read (stamp even on failure) |

### Matching

Match Zoho `Site_Location` (free-text address) to portal `street` + `city`.
Normalize: strip unit/floor suffixes (`FL`, `UNIT`, `APT`, `BLDG`, `BSMT`, `LOT`),
expand street-type abbreviations, drop directionals. Customer NAME is unreliable
as a key -- portal records appear under spouses and co-owners (Kerschner filed as
Patricia, Casey as Crystal, Ocasio as Aileen Encinas, Webb as Jeffrey) and contain
typos (Khatun/KHATUM). Name is a fallback only. Note `28-30 ABERDEEN ST` normalizes
badly (house number parses as "28", remainder keeps "30") -- handle hyphenated ranges.

### IC_Action_Required rule

True when the portal status contains any of: `Corrections`,
`Customer Action Required`, `InComplete`, `Unsubmitted`, `On Hold`,
`Awaiting Meter Fee`. False for `Re-Review` (utility is working).
Also true when no portal application is found at all for an active CT
project (see territory rule below -- out-of-state installs are exempt).

**2026-09-17 revision:** was `Corrections Required` (exact phrase); changed to
bare `Corrections` after the sync's first dry-run flagged Ramsey Goodrich
(`Pending Witness Test Corrections`) as a disagreement -- that status is a
real blocker and the narrower phrase missed it. Confirmed: every portal
status containing `Corrections` is a blocker.

### Territory rule

The scrape only covers UI (CT) and Eversource-CT, so an install outside
Connecticut will never have a portal record -- without an exemption it would
show `NO APPLICATION FOUND` and `IC_Action_Required = true` forever, a
permanent false alarm, not a real blocker.

Detect territory from the state parsed out of `Site_Location`. Only fall back
to `Utility_Provider` (checked for "eversource" or "illuminating") when the
address didn't yield a recognizable state at all -- a state that parsed and
just isn't CT is a real answer, not a parse failure, so it does not fall
back. Out-of-territory installs get `IC_Portal_Status = "Outside
UI/Eversource territory - not tracked by this sync"`, `IC_Action_Required =
false`, and still get `IC_Portal_Checked` stamped like any other record.
Found via the sync's first dry-run: Michael Duke (South Salem, NY) and Alex
Starr (Scarsdale, NY) were both flipping to a false "action required" alarm
before this rule existed.

### Matching priority (revised 2026-09-17, after the first live run)

Try `IC_Project_Number` first when it's set -- it's an exact key the utility
assigned, not a heuristic. Only fall back to address matching when it's
blank. A number that IS set but not found in the current scrape is **not**
retried by address -- the number is authoritative, not a hint.

Found via the first live run: Mohammed Alamgir | 28 (`INT-119959`) and Chris
Anderson (`DER-55435`) both carry the correct project number, but
address-only matching failed on both (28 vs the portal's combined "28-30
Aberdeen" record; 153 vs the portal's 155 Cowles St) and overwrote a real
status with `NO APPLICATION FOUND`. Matching by number first fixes both.

### Never overwrite a real status with "no application found"

If a CT, non-commercial install had a real matched status before and has no
match this run, that's a **matcher failure**, not evidence the utility
dropped the record -- "I can't find it" and "the utility has no record" are
different facts, and before this fix they produced identical output. Keep
the previous `IC_Portal_Status` and `IC_Action_Required` untouched (nothing
gets written differently) and log it as a regression for manual review
instead. This only applies when the existing status is a real one -- a
record that was already one of this sync's own placeholders (`NO
APPLICATION FOUND...`, `Outside UI/Eversource territory...`, `Commercial -
not tracked...`) just gets re-evaluated normally.

### Commercial installs

Commercial jobs follow a different interconnection process and often have no
record in either portal at all -- same false-alarm problem as out-of-state,
same fix shape: only matters when there's **no portal match**. A commercial
install that DOES match (Carl Guild, `INT-117499`) syncs completely
normally.

Signal used: **`Property_Type`**, a real-estate/parcel-data field already on
Installs -- not matching the word "Commercial" in `Name`. Verified against 5
real records: residential installs all read `SINGLE FAMILY RESIDENCE`;
commercial ones read `COMMERCIAL`, `OFFICE BUILDING`, and `EXEMPT` (a
tax-exempt org -- not literally the word "commercial", which is why an
allowlist of commercial-sounding values would have missed it). Classified by
**absence** of a residential marker (`RESIDEN`/`FAMILY`/`CONDO`/`TOWNHOUSE`/
`DUPLEX`/`TRIPLEX`) rather than presence of a commercial one, since the
commercial side has many more possible values than the residential side. A
blank `Property_Type` is treated as residential (no signal either way, don't
guess commercial from silence).

Commercial + no match gets `IC_Portal_Status = "Commercial - not tracked by
this sync"`, `IC_Action_Required = false`, same treatment as out-of-territory.

### Sync rules (non-negotiable)

1. NEVER overwrite a non-empty `IC_Project_Number`. Fill blanks only; on conflict,
   record it in `IC_Portal_Status` and flag -- do not silently change.
2. Notify only on CHANGE (false->true on `IC_Action_Required`, or status string
   change). A daily restatement of unchanged blockers gets ignored within a week.
3. Always stamp `IC_Portal_Checked`, including on failure -- a stale timestamp is
   the only signal that the sync itself broke.
4. All Zoho writes send `"trigger": []` and skip `assignment_rules` /
   `connected_workflows`. Note: `skip_feature_execution` accepts max 2 entries.
5. **Not off-hours as of 2026-09-18** -- schedule is midday (13:00 ET / 17:00
   UTC), moved deliberately. Each run still evicts whoever is logged into
   PowerClerk -- one session per account -- but now that risk lands during
   the workday instead of before anyone's in the portal. See the Alerting
   section below for the current times and why.

### Open items needing a human, do not auto-resolve

- **Daniel DeTuccio** (18 Bergen Lane, Wolcott) -- at Inspection stage, install
  date 2026-01-06, but NO interconnection application exists in either portal.
- **Mohammad Gafur** (46 Fairfield Ave, Stamford) -- portal record exists but is
  `Unsubmitted` with no project number since 2026-05-07.
- **Chris Anderson** -- Zoho says 153 Cowles St, portal says 155. Unresolved; a
  note is parked in that record's `IC_Portal_Status`.
- **Frank LaFauci** -- two Eversource records at 130 Shelter Rock Rd: INT-107305
  (Online 2025) and INT-116999 (awaiting town approval). Possibly a second array.
- **John Fiorello** -- INT-117227 is 24 Brightwood (matches Zoho). INT-117263 is
  22 Brightwood and has no Zoho record; unclear if it is also ours.

## Zoho OAuth scope constraint (verified 2026-09-17)

The connected app grants record-level scope only (`ZohoCRM.modules.ALL`).
It does **not** grant `ZohoCRM.settings.fields.READ` or `ZohoCRM.coql.READ`;
calls to `/crm/v7/settings/fields` and `/crm/v7/coql` return
**HTTP 401 OAUTH_SCOPE_MISMATCH**.

Do not re-authorize the app to add scopes. Re-minting the refresh token
invalidates the current one, which is shared with the Render service
(`aurora-zoho-sync.onrender.com`) and the `sync-aurora-users` /
`sync-hea-status` workflows. Everything the sync needs works within
`ZohoCRM.modules.ALL`:

| Instead of | Use |
|---|---|
| `POST /crm/v7/coql` | `GET /crm/v7/Installs/search?criteria=(...)` |
| `GET /crm/v7/settings/fields` | request the field in `?fields=` on a record read |

Read/write on `Installs` (GET, PUT, POST) is confirmed working under this scope.

## Zoho PUT gotcha

A `PUT /crm/v7/Installs` with `{"data": [{"id": "..."}]}` and no other fields
returns **HTTP 200 and is accepted as a real write** -- it does not error as
"no updatable data". It bumps `Modified_Time` / `Modified_By` on that record.
Never use a bare-id PUT as a permission probe or a dry run. `ZohoCRM.modules.ALL`
grants read and write together, so a successful read already proves write access.

## Alerting (SMS to Harry)

Two separate workflows.

| Workflow | Cron (UTC) | ET | Does |
|---|---|---|---|
| `powerclerk-sync.yml`   | `0 17 * * *` | 13:00 | Scrape both portals, write `IC_Portal_*` |
| `powerclerk-notify.yml` | `0 18 * * *` | 14:00 | Read Zoho, send one SMS digest |

**Schedule moved to midday 2026-09-18** (was 05:00 / 09:00 ET, off-hours). This
is a deliberate tradeoff, not an oversight: the sync **will now evict whoever
is logged into PowerClerk during the workday** -- it no longer avoids that by
running before anyone's around. The 1-hour gap between the two crons is
deliberate too, independent of the off-hours question: the scrape takes ~10
minutes, and the notifier must not read Zoho while the sync is still mid-write
-- it would see a partial mix of updated and stale records and could send a
digest based on data that's about to change again in the same run.

Splitting sync and notify into separate workflows (rather than one job) still
matters for its original reason too: a failed scrape still lets the notifier
run and report that `IC_Portal_Checked` has gone stale.

**Both crons enabled 2026-09-17**, after: the sync's live-run diff was reviewed
and matched expectations (including the IC_Project_Number-first / no-overwrite
/ commercial-exemption fixes), and the notifier's dry-run digest, the
`IC_Alert_Sent` backfill, and a real test SMS were all verified per the test
order above.

### Credentials

```
op://Helio Automation/Twilio/TWILIO_ACCOUNT_SID
op://Helio Automation/Twilio/TWILIO_AUTH_TOKEN
op://Helio Automation/Twilio/TWILIO_FROM_NUMBER
op://Helio Automation/Twilio/ALERT_SMS_TO
```

`ALERT_SMS_TO` may hold a comma-separated list; split on commas and send to each.
Reuse `_send_sms()` from `main.py` rather than writing a new Twilio client.

### Field: IC_Alert_Sent (datetime)

Tracks what has already been texted, so a long-running blocker alerts once.

- Notify where `IC_Action_Required = true` AND `IC_Alert_Sent` is null.
- Stamp `IC_Alert_Sent` only after Twilio confirms the send. Never stamp first --
  a failed send that stamped would silently swallow the alert forever.
- The 5am sync CLEARS `IC_Alert_Sent` whenever `IC_Action_Required` goes true->false,
  so a project that gets blocked again later alerts again.

### Field: IC_PTO_Alert_Sent (datetime) -- PTO milestone (2026-09-17)

PTO (permission to operate) is good news, but it's the activation milestone,
so it belongs in the same digest as blockers, not silence. Mirrors
`IC_Alert_Sent`'s design exactly, except it is never cleared -- PTO doesn't
reverse, so there's no "re-block" case to reset for.

- The 5am sync fills `Utility_PTO` with the portal's status date when
  `IC_Portal_Status` becomes UI's `Energize System` or Eversource's `Level 1
  - Online` **and** `Utility_PTO` is currently empty. Never overwrites a
  non-empty `Utility_PTO` -- it may have been entered by hand (David).
- The 9am notifier announces where `Utility_PTO` is set AND `IC_PTO_Alert_Sent`
  is null, stamping it only after a confirmed send, same rule as `IC_Alert_Sent`.
- **This field does not exist in Zoho yet** -- confirmed empirically, not
  assumed (see the gotcha below). `powerclerk_notify.py` detects that at
  startup and disables PTO announcements for the run, logging a note -- the
  blocker digest is unaffected either way.
- **Before relying on live PTO announcements:** create `IC_PTO_Alert_Sent`
  (datetime) on Installs, then run `powerclerk_notify.py
  --backfill-pto-alert-sent` once. Several installs already have (or will
  shortly get, via the sync's new fill logic) `Utility_PTO` set from before
  this feature existed -- without the backfill they'd all announce at once
  the first time the notifier runs with the field present.
- First install expected to get `Utility_PTO` set by this logic: Mohammed
  Alamgir | 28 (`INT-119959`), which reached `Level 1 - Online` on 2026-09-17.

### Zoho gotcha: an unknown field name does NOT error, on read OR write

Verified 2026-09-17 while building the above, the hard way: a GET's `fields=`
naming a field that doesn't exist returns **HTTP 200**, silently omitting it
-- it does not 400 the way a genuinely malformed request does (contradicting
what earlier smoke-test comments assumed, which had just never actually been
tested against a truly nonexistent name). Worse: a **PUT** naming an unknown
field in `data` also returns **HTTP 200, `"status":"success"`, `"message":
"record updated"`** -- because the record ID itself is valid, Zoho legitimately
updates `Modified_Time` and reports success, while silently dropping the one
key it didn't recognize. `powerclerk_notify.py`'s first version probed with a
scoped `fields=id,IC_PTO_Alert_Sent` GET, got HTTP 200, concluded the field
existed, and "backfilled" 12 records that reported `"success"` -- all silently
no-ops. Caught before the notify cron's first real firing only because a
direct-by-ID GET (no `fields` param, which returns every real field including
nulls) showed the key was never actually present.

**The only reliable existence check found:** fetch one real record directly
by ID with no `fields` param, and check whether the field name is a literal
key in the response dict. Never infer existence from a 200 on either a
scoped GET or a PUT -- both succeed unconditionally regardless of whether
the field is real.

### Message rules

1. **One SMS per run**, a digest -- never one per project. Blocked and PTO
   items share one message, in labeled sections.
2. **Cap at 8 total new events** (blocked + PTO combined). More than 8 in one
   run means a scraper bug, not that many things happening overnight. Send
   `"N new events -- check Canvas, likely a sync issue"` and stamp nothing,
   so the real alerts survive for the next run once the bug is fixed.
3. **Send nothing when there is nothing new.** No "all clear" texts.
4. If `IC_Portal_Checked` is older than 36h on most active records, send
   `"Portal sync stale since <date>"` instead of a digest -- the data is not
   trustworthy enough to alert on.

### DST caveat

GitHub cron is UTC and does not shift. `0 18 * * *` is 14:00 EDT but 13:00
EST -- the wall-clock ET target moves an hour earlier in winter. Either
accept the winter hour or have the notifier check ET local time and exit if
it's before the target, with the cron set an hour early year-round.

**Decision:** accept the winter hour, same reasoning as before the schedule
moved -- a hard "exit if before target" gate on a single daily trigger would
mean it never fires at all in winter, since there's no later same-day
trigger to catch the real mark.

**What DOES hold regardless of DST:** the 1-hour gap between the sync
(`0 17 * * *`) and notify (`0 18 * * *`) crons. Both shift by the same hour
together, so the sync always finishes with room to spare before the
notifier reads, in both EDT and EST -- the gap that actually matters for
correctness (not reading mid-sync) is independent of what the wall-clock ET
time happens to be that day.

## Sync + notify implementation (2026-09-17)

Built `scripts/powerclerk_sync.py` (+`.github/workflows/powerclerk-sync.yml`)
and `scripts/powerclerk_notify.py` (+`.github/workflows/powerclerk-notify.yml`),
per the rules above. (Crons as first shipped: `0 9 * * *` / `0 13 * * *`,
off-hours -- moved to the midday schedule in the Alerting section above on
2026-09-18.) Both use `/search`, never `/coql`. `powerclerk_sync.py` imports
`scrape_all_projects()` from `powerclerk_audit.py` rather than duplicating the
login/replay logic. `powerclerk_notify.py` imports `_send_sms` directly from
`main.py` (repo root added to `sys.path`) -- confirmed safe to import
standalone: no module-level network calls or scheduler side effects outside
the FastAPI `startup` event. `_send_sms` was changed to return the Twilio
`sid` (or `None`) instead of nothing, so a caller can tell whether the send
was actually confirmed -- existing callers that ignored the return value are
unaffected.

**Both crons are commented out for now.** Test order before enabling them:
1. `powerclerk_notify.py --dry-run` -- prints the digest it would send.
2. `--backfill-alert-sent` -- stamps `IC_Alert_Sent=now` on every
   currently-blocked record, so the first live run doesn't page on
   pre-existing blockers.
3. `--test-sms "..."` -- one real, fixed-text send to confirm Twilio delivery
   without touching Zoho.
4. `powerclerk_sync.py --dry-run` -- still logs into PowerClerk for real (the
   scrape is what produces the values to diff), but writes nothing to Zoho.
   Diffs derived `IC_Portal_Status`/`IC_Action_Required` against what's
   already stored and reports every disagreement. This is the one that
   matters: this morning's 45 hand-verified records are a known-good
   reference that stops being trustworthy the moment the sync runs live, so
   this is the only chance to catch a matcher regression against it.
5. Only then uncomment the `schedule:` blocks in both workflow files.

All workflow_dispatch inputs default to the safe choice (`dry_run: true`,
`backfill_alert_sent: false`, `test_sms: ''`).

### Address matching, as implemented

`normalize_street` / `normalize_city` in `powerclerk_sync.py` handle the cases
called out above: unit/floor suffix stripping, street-type expansion (ST ->
STREET etc.), leading directional removal, and keeping a hyphenated
house-number range (`28-30`) as one token. Verified against the specific
addresses in this file's Open Items list (28-30 Aberdeen, 130 Shelter Rock
Rd) before the first live run. `Site_Location` is assumed to be
`"street, city[, state zip]"` -- not yet verified against real data, since
nothing in this repo can query Zoho outside of GitHub Actions. If the
`--dry-run` diff comes back full of false disagreements, check this
assumption first.

## Phase 2: pulling the actual correction text (verified in Chrome 2026-09-17)

The grid gives the status; the *reason* lives in each project's Communications
list. Both portals work identically. Confirmed on DER-55743 (Feola, UI) and
INT-119883 (Mabud, Eversource).

### URL patterns -- identical shape on both hosts

```
/MvcProjects/LandingPage?ProgramId=<PID>&ProjectId=<OPAQUE>
/MvcProjects/ViewCommunication?ProgramId=<PID>&ProjectId=<OPAQUE>&CommunicationId=<OPAQUE>
```

`ProjectId` is an opaque 12-char token, **not** the DER-/INT- number. It is not
in the grid payload. Get it by opening the row's expander and following
"View/Edit Project", or check whether `GetProjectList3` returns it under another
key before resorting to clicking.

Both pages render as plain server-side HTML -- `get_page_text` returns the full
body. No JS rendering, no PDF. The correction text is inline in the email body.

### Where the reason lives

Landing page has a `Communications Sent to installs@helio.solar` table:
Date | Status | Subject | View. Newest first. The View button opens
`ViewCommunication` in a NEW TAB (target=_blank) -- handle the tab, or fetch
the URL directly once the CommunicationId is known.

Subjects that carry corrections:
- UI: `Application Validation ON HOLD - Response Required for <PROJ> <ADDRESS>`
- Eversource: `Action needed on application update for project #<PROJ>, <NAME>`

### Parsing

UI bodies have a literal `Corrections Required:` header followed by one line per
item -- parse that block. Eversource writes prose with no delimiter; take the
paragraphs between the opening "some required information was missing" sentence
and the closing "Please log in to this customer's application" sentence.

Store in a new long-text field `IC_Correction_Detail` (create it; does not exist
yet). Also store the communication date -- it differs from the grid's status
timestamp and is the real "how long have we been sitting on this" clock.

### Better signal than string matching

Both portals have a built-in filter tab that already isolates blocked projects,
which beats matching on status substrings:

- UI: `Application Corrections Required` (returned exactly Feola + Webb),
  plus `Technical Review Feedback`. UI's Program Home also shows live counts
  per view.
- Eversource: `Customer Action Required` and
  `Impact Study in Process- Customer Action Required`.

Consider driving `IC_Action_Required` off view membership instead of, or as a
cross-check on, the substring rule.

### Also noted

Eversource's project list has an **Export to CSV** button. Worth testing --
it may replace the `GetProjectList3` replay entirely.

### Investigation results (2026-09-17, before building sync integration)

**ProjectId is already in the grid -- no clicking needed.** Each
`GetProjectList3` row is `{ProjectData, ProjectId, StatusId, CanDelete,
CanAssign, QueuePosition, Highlights}` -- `ProjectId` sits right alongside
`ProjectData`. `powerclerk_sync.py` can build
`/MvcProjects/LandingPage?ProgramId=..&ProjectId=..` directly from the same
scrape it already does; the grid's own `<a>` tags are 0 (it's a Vue app, no
real row links) so clicking was never going to work anyway.

**Export to CSV: inconclusive, not worth pursuing.** The button is real and
clickable, but firing it produces neither a download event nor a new tab
within 40s, tried twice. Moot now that ProjectId comes from the grid directly.

**The Communications table is real and reachable without any clicking most
of the way.** Landing page has 6 `<table>` elements; the Communications one
(Date / Status / Subject) reads fine via plain DOM/text scraping -- confirmed
against DER-55749 (Jeffrey Webb), whose newest entry read `9/15/2026 12:05:55
PM | Received | Response Required: 5 BOYSENBERRY LN, SHELTON CT 06484,
Project ID # DER-55749`. So **the notice date and subject are available with
zero extra permission** -- only the message body requires opening
`ViewCommunication`.

**Blocker: opening a communication requires MFA, confirmed on both portals.**
"View" is a `<button data-test-role="view-communication-button">`, not an
`<a>` -- clicking it (after dismissing a "What's new?" onboarding modal that
otherwise intercepts every click) throws: *"Multi-Factor Authentication
Required -- Your account must have Multi-Factor Authentication enabled in
order to perform this operation."* No new tab opens, no network request
fires -- the app refuses client-side before anything is requested. Verified
on a real blocked project on **both** UI (DER-55749) and Eversource
(INT-121765). Login and grid browsing do NOT require MFA; only opening a
communication's body does.

This means `IC_Correction_Detail` (the verbatim ask text) cannot be
populated without either enabling MFA on the PowerClerk service account(s)
and teaching the automation to supply a TOTP code (a 1Password TOTP field
plus a `pyotp`-based step in the login flow), or some other access path not
yet found. `IC_Correction_Date`, however, needs no such access -- it can be
read straight off the Communications table by matching Subject text against
the known correction-notice patterns (`Application Validation ON HOLD -
Response Required` / `Action needed on application update`), which the
existing scrape-and-login flow already reaches.

### Live examples captured

DER-55743 Feola, UI, "Validation On Hold | Corrections Required" since 9/8:
  - existing system size does not match UI records
  - Total System Size (AC_kW) needed on the line diagram
  - wiring schematics blurry, upload a clear image
  NOTE: 9/3 on hold -> 9/4 corrections submitted -> 9/8 on hold again. Bounced twice.

INT-119883 Mabud, Eversource, on hold since 5/26 (114 days):
  Eversource cannot verify the scheduled HES date of 10/15/2026. Needs the HES
  audit project #, and the HES vendor must enter the scheduled date into
  Eversource's internal Tracksys before review resumes.

### Fields for correction detail (created 2026-09-17)

| API name | Type | Meaning |
|---|---|---|
| `IC_Correction_Detail` | textarea (large, 32k) | Verbatim ask from the utility's correction notice |
| `IC_Correction_Date`   | date | Date the notice was sent -- the real aging clock |

Already backfilled by hand on Gene Feola (`5264387000095379017`) and
Abdul Mabud (`5264387000086240028`). Use those two as the formatting reference:
a one-line header naming the utility and notice type, then numbered asks, then
any note about who the actual blocker is.

Sync behaviour:
- Populate both only when `IC_Action_Required` is true.
- When a project stops being blocked, CLEAR both, same as `IC_Alert_Sent`.
  Stale correction text on an unblocked project is worse than none.
- If the newest correction notice is newer than `IC_Correction_Date`, replace
  the detail and re-alert -- a second round of corrections is new news even
  though `IC_Action_Required` never flipped false. Feola bounced 9/3 -> 9/4 ->
  9/8 and a naive false->true check would have missed the second round.
- Include the first ~2 asks in the SMS, truncated. Full text lives in Zoho.
