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

## LightReach cash model -- findings 2026-09-18

Verified end to end: a payment was predicted from `LightReach_MilestoneLog`
alone and matched the actual deposit (Ryan Hopley, activation approved Mon
2026-09-14 17:36 ET, paid Thu 2026-09-17, $7,447.60 = 20% of 37,238).

### The real payment-date rule (Harry, confirmed)

Milestone `approved` in `LightReach_MilestoneLog` is the trigger:
- approved by EOD Tuesday -> paid Thursday that same week
- approved Wednesday through Friday -> paid the following Tuesday

This is NOT what the cashflow code does today. `main.py` derives dates from
Substantial Completion: `payment1 = next Monday on/after SC+14`,
`payment2 = next Monday on/after SC+33`, and when SC is missing it is
*projected from the current stage*. Three stacked estimates where the lender
gives an exact timestamp. Replacing that is the core of the rebuild.

### Warranty reserve ($250) is inverter-dependent

`CASHFLOW_LR_WARRANTY = 250.00` is currently deducted from the 20% final on
EVERY LR project. It should depend on the inverter:

| Inverter | LR holds reserve? |
|---|---|
| Q.Tron / microinverters | NO |
| SolarEdge | YES |
| Tesla | YES |

Derivable from the `Inverter_Model` field on Installs. Harry has accepted the
blanket deduction for now -- it understates the forecast by $250 on
micro-inverter projects, which errs in the safe direction, and more projects
using reserve-bearing inverters are expected going forward.

### Materials is the largest error source

`materials_est = system_watts * CASHFLOW_MATERIALS_PPW ($1.26/W)`, a flat
estimate, deducted from the 80% draw. Three different figures exist per project:

| | Bishuja Deb | Daniel Ghazal |
|---|---|---|
| model estimate @ $1.26/W | 18,963.00 | 33,736.50 |
| Zoho `Equipment_Invoice_Total` (Rob's entry) | 17,268.74 | 27,341.29 |
| Harry's figure | 17,684.71 | 23,059.57 |

Ghazal spans $10,677. Because the estimate is deducted, an overstated estimate
forecasts the draw LOW by that amount. This is the main reason the overrides
tab exists. UNRESOLVED: which figure LightReach actually deducts, and where
Harry's numbers come from.

### DC holdback (already correct in code)

`$0.20/W` for projects created on/after 2026-03-06, `$0.05/W` before
(`_lr_holdback_ppw`). Paid ~25 days after the activation payment. It is an
additional revenue event, never netted against anything -- a LightReach
incentive for using domestic-content equipment. Verified: Deb 15.05 kW ->
$3,010.00 exactly.

Note: DC holdback appears in NO webhook event and has no Zoho field. It exists
only in this calculation and in the Google Sheet it writes
(`CASHFLOW_SHEET_ID = 1ktCKriA4W97Cxy-bubTD2zSP8W1X8fP52BLXvElkp5g`).

### Webhook events that never fire

`directPayEvent` and `allConsumerTaskEvents` are both subscribed with zero
failures and have NEVER delivered an event. Handlers for both are written and
idle. Consequence: no payout confirmations, and rejections are visible as a
status change with no reason attached. Open question to the LightReach rep.

## LightReach Funding page -- the real source of truth (verified 2026-09-18)

`https://palmetto.finance/accounts/{LightReach_Account_ID}/funding`

Zoho already stores `LightReach_Account_ID` on Installs, so the URL is
deterministic. No MFA on LightReach (as of 2026-09-18), Auth0 login, so this is
automatable the same way PowerClerk is: 1Password -> Actions -> Playwright.

**Scraping notes:** server-rendered, no XHR to intercept -- `get_page_text`
returns everything. BUT the funding panel loads asynchronously and took >10s on
one account. Wait for the literal text `PAYMENT PLAN` to appear; do not use a
fixed sleep.

### What the page gives you (none of it available from webhooks)

- Payment plan: Install Approved $ / %, Activation Approved $ / %, **Vendor
  Direct Pay** (the real materials figure)
- Total Project Value, Payout to-date, Amount Remaining
- Total Holdback / DC Holdback / Standard Holdback
- Full transaction ledger: event date+time, payout event, description, **BATCH**,
  **STATUS**, amount
- Pricing settings: Standard Holdback $/W, Domestic Content Modifier $/W,
  DC Holdback $/W, pricing lock date
- Invoices (supplier invoice numbers + PDF filenames), quotes, EPC rates

### BATCH = the actual payment date

Confirmed four times against the Tue/Thu rule:
- Ghazal install approved Mon 03-09 -> batch 03-12 (Thu)
- Ghazal activation approved Thu 09-10 -> batch 09-15 (following Tue)
- Hopley install approved Mon 08-24 -> batch 08-27 (Thu)
- Hopley activation approved Mon 09-14 -> batch 09-17 (Thu), deposit confirmed

### Materials: Vendor Direct Pay, and why every other number is wrong

LightReach pays the supplier DIRECTLY (Greentech-CED, US Electric Services --
shown as "Direct Pay: Active - <vendor>" on the account). So the deduction is
LightReach's figure, not Rob's invoice and not a $/W estimate.

**Critical constraint (Harry):** Vendor Direct Pay does not exist in LightReach
until equipment is ordered, which is right before install. For the whole ~45-day
pre-install pipeline -- the part that matters for forecasting -- there is no
actual figure. So the estimate cannot be removed, only tiered:

1. no quote yet -> $/W estimate (covers most of the pipeline)
2. supplier quote entered -> `Equipment_Invoice_Total`
3. LR materials invoice fires -> Vendor Direct Pay (actual)

Surface which tier a forecast is using, so guesses are visibly guesses.

### The $1.26/W constant is not calibrated

| | Vendor Direct Pay | System | actual $/W |
|---|---|---|---|
| Ghazal | 23,059.57 | 26.775 kW (22 kW contracted) | 0.86 - 1.05 |
| Hopley | 12,237.45 | 8.6 kW | 1.42 |

Neither has a battery, so that is not the driver. Likely equipment architecture:
Ghazal is Hyundai modules + 2 SolarEdge string inverters; Hopley is Q.TRON BLK
**AC** modules (integrated microinverters -- Deb shows 35 modules / 35
"inverters", same module). AC modules cost more per watt.

**A flat $/W also cannot represent storage at all.** Recommended shape:

```
materials_est = (pv_watts * rate_by_equipment_type) + (battery_kwh * rate_per_kwh)
```

`Battery_kWh` is populated and reliable on Installs (Powerwall 3 13.5, Fortress
Avalon 19.6, Franklin 13.6, up to 40.96). `Battery` (name) is sometimes null
where `Battery_kWh` is set -- use the kWh field as the indicator.

Rates must come from calibration, not guesswork. See the proposed first build.

### DC holdback timing -- the code is wrong

Harry: **LightReach approves the holdback 21 days after ACTIVATION IS APPROVED.**
Until then the ledger row shows STATUS = `PAUSED` with no batch date. That is
normal, not a problem.

Worked example (Hopley): activation approved Mon 2026-09-14 -> +21d -> approved
Mon 2026-10-05 -> Tue/Thu rule -> paid **Thu 2026-10-08**.

`main.py` currently computes `holdback_date = payment2 + 25 days` = 2026-10-12,
a Monday, which is not a batch day. Three errors: wrong anchor (payment date
instead of approval date), wrong offset (25 vs 21), no batch-day rounding.

Also: DC Holdback $/W is a per-project pricing setting visible on the page
($0.20 on Hopley). Prefer reading it over inferring from the 2026-03-06 cutoff.

### Clawbacks and true-ups exist

Ghazal: install paid 2026-03-12, **clawed back in full 2026-07-04**
("installApproved clawback triggered by install rule"), trued up and re-paid
2026-09-15. Five months of revenue booked that was not held. The cashflow model
has no concept of a clawback and would never have shown it leaving.

### Warranty reserve is not purely inverter-model-driven

Ghazal: -$450.00, "Inverter warranty reserve for SolarEdge USE11400H-USSKBEZ8
(2)" -- $225/inverter, matching `Inverter_Model` + `Inverters` in Zoho.
Hopley: same USE11400H family, 1 unit, and **no warranty line at all**.
So something beyond the model string decides. Open question for LightReach.

### Proposed first build (not the forecast engine)

Scrape the funding page for every already-installed LR project. That yields,
in one pass:
1. calibration data for the materials estimate (real $/W, segmented by
   equipment type and storage)
2. how close `Equipment_Invoice_Total` runs to Vendor Direct Pay, which decides
   whether tier 2 above is trustworthy
3. a reconciliation of actual payments vs what the sheet forecast
4. any paused or unpaid holdbacks -- Hopley has $1,720 pending right now

### One batch calendar, not three payment rules

Harry confirmed: EVERY payout type follows the same schedule. A payout becomes
payable on its approval date, then lands on the next batch:

    approved by EOD Tuesday      -> paid Thursday that same week
    approved Wednesday-Friday    -> paid the following Tuesday

The only thing that differs per payout type is what triggers the approval:

| Payout | Approval trigger |
|---|---|
| Install (80% less Vendor Direct Pay, less warranty reserve) | milestone `install` -> `approved` |
| Activation (20%) | milestone `activation` -> `approved` |
| DC holdback (watts x DC $/W) | activation approval + 21 days |
| Clawback / true-up | ad hoc, LightReach-initiated |

So implement ONE function -- `batch_date(approval_date)` -- and apply it to all
of them. Do not write separate date logic per payout type; that is what produced
the current three-different-lags model.

## LightReach funding audit built (2026-09-18)

`scripts/lightreach_funding_audit.py` + `.github/workflows/lightreach-audit.yml`
(workflow_dispatch only, no cron -- this is a one-shot data pull, not a
recurring sync). Read-only: writes nothing to Zoho or LightReach. Queries
Zoho for the 136 Installs with `LightReach_Account_ID`, logs into
palmetto.finance once via Auth0 (identifier-first flow: email field, submit,
password field, submit -- worked first try, no MFA), and scrapes each
account's `/funding` page. Outputs `artifacts/lr_funding_projects.csv` (one
row per project) and `artifacts/lr_funding_transactions.csv` (one row per
ledger line). Final run: **136 ok, 0 skipped, 0 failed, 171 ledger rows.**

### Two async-render timers, not one

The `PAYMENT PLAN` panel and the transaction ledger `<table>` render on
*separate* timers. Waiting only for `PAYMENT PLAN` text (as CLAUDE.md's
original note specified) let the scrape proceed while the ledger table was
still empty -- confirmed on Ryan Hopley, whose payout-to-date proved the
ledger had real rows, but the first pass extracted 0. Fixed by adding a
second, independently-bounded wait for `table tbody tr` (15s) after the
`PAYMENT PLAN` wait succeeds -- but not treating its absence as a failure,
since a genuinely new account can have zero transactions.

### The first-run skip pattern was rate-limiting, not per-account slowness

First 136-account run: 107 ok, 29 skipped ("PAYMENT PLAN never appeared
within 45000ms"), spaced with suspicious regularity (~every 4-5 accounts
regardless of which one) -- including Ryan Hopley, the one account already
verified by hand. Added a retry (2 attempts) before logging a real skip.
Second full run with the retry: all 4 accounts that hit a timeout succeeded
on the retry, and the final run had **zero** skips. Confirms it was
transient (rate-limiting or a flaky response), not a real per-account
rendering problem -- don't raise the timeout further if this recurs, retry
instead.

### Ledger column order (confirmed, not assumed)

`EVENT DATE & TIME | PAYOUT EVENT | DESCRIPTION | BATCH | STATUS | AMOUNT` --
matches what the script's positional mapping already assumed, confirmed via
a live debug dump of the actual `<table>` header before trusting it.

### Cross-validated against known cases

Hopley's scraped figures exactly match the ones already verified by hand
elsewhere in this file ($29,790.40 install / $7,447.60 activation /
$12,237.45 Vendor Direct Pay / $37,238.00 total, fully paid). Ghazal's
ledger reproduces the clawback/true-up story exactly (install approved
03-12, clawback event Jul 4, true-up + activation both batched 09-15).

### Debug tooling kept in the script (not thrown away)

`LR_ONLY` env var / `only` workflow input restricts the run to specific
`LightReach_Account_ID`s, comma-separated -- for re-testing a handful of
accounts without a full ~28-minute, 136-account run. The first account in
any run (or any account when `LR_ONLY` narrows to one) prints its raw page
text and `<table>` structure to the log.

## Funding audit results (136 projects, 171 transactions, 2026-09-18)

Source: `artifacts/lr_funding_projects.csv`, `artifacts/lr_funding_transactions.csv`.
38 of 136 have a Vendor Direct Pay figure (the rest predate Direct Pay, are
cancelled, or have not ordered equipment yet). That 38 is the calibration set.

### Batch calendar -- confirmed at scale

Batch dates are **only ever Tuesday or Thursday** (98 Tue / 68 Thu across 166
transactions). Not an approximation.

`batch_date(approval_date + lag)` where lag is 0 for INSTALL APPROVED,
ACTIVATION APPROVED, MATERIALS INVOICE and WARRANTY: **140 of 155 exact**.
Residuals: 5 at +2d and a handful larger (one batch slipped, plus Ghazal's
+63d clawback which is ad hoc).

### DC holdback lag is 23 days, not 21 and not 25

Measured by sweeping the lag against 11 paid holdbacks:

| lag | matches |
|---|---|
| 21d | 1/11 |
| 22d | 5/11 |
| **23d** | **10/11** |
| 24d | 9/11 |
| 25d (current code) | 8/11 |

Use `batch_date(activation_approval + 23 days)`.

### Materials: the $1.26/W constant is ~7% high

| segment | n | median $/W | mean | min | max |
|---|---|---|---|---|---|
| AC-module, no battery | 13 | 1.201 | 1.242 | 1.024 | 1.672 |
| string/other, no battery | 25 | 1.109 | 1.140 | 0.861 | 1.545 |
| ALL | 38 | 1.173 | 1.175 | 0.861 | 1.672 |

Because materials is DEDUCTED, an overstated rate forecasts the draw LOW.
Segmenting AC-module vs string is worth ~$0.09/W.

**Caveat that matters: not one project in the calibration set has a battery.**
So there is no empirical basis for a `$/kWh` storage term yet. Any battery
project remains a guess until some land in the data. Do not invent a rate.

The spread is wide even within a segment (0.86-1.67). Pre-quote forecasts
should be presented as a band, not a point.

### Rob's Equipment_Invoice_Total is trustworthy -- use it as tier 2

Compared against Vendor Direct Pay on 35 projects:
**median error 0.0%, 29 of 35 within +/-3%.**
Outliers: Margarita Cayo -95.6% (Zoho value effectively missing) and
Daniel Ghazal +18.6% (known problem project).

So: estimate -> `Equipment_Invoice_Total` as soon as Rob enters it ->
Vendor Direct Pay once LightReach posts it. Tier 2 is nearly as good as actual.

### Warranty reserve: a per-inverter rate table

Confirmed across 27 warranty lines. Rate depends on inverter model, multiplied
by the number of inverters:

| Inverter | $/unit |
|---|---|
| Tesla (any) | 600 |
| SolarEdge SE7600H / SE10000H / SE11400H / USE11400H / USE7600H / SE5700H | 225 |
| SolarEdge SE3800H / SE6000H | 170 |
| Microinverters / AC modules (Q.TRON etc) | 0 (no warranty line) |

The flat `CASHFLOW_LR_WARRANTY = 250` is wrong in both directions.

**Data-quality warning:** Zoho's `Inverter_Model` is not reliable. Hopley is
recorded as `USE11400H-USMNBE78` x1 but has NO warranty line and Q.TRON AC
modules -- the field does not reflect what was installed. Some projects have
multiple warranty lines for mixed inverter types (Boris Zarate: 450 + 170).

### Transaction status semantics (Harry, confirmed)

| Status | Meaning |
|---|---|
| `OPEN` | Approved, not yet assigned to a batch. Follows the normal Tue/Thu rule. |
| `APPROVED` | Batched / paid. Has a batch date. |
| `PAUSED` | DC holdback inside its 23-day post-activation wait. Normal. |

None of these indicate a problem. An OPEN row simply has not been batched yet --
predict its date exactly as for any other approval.

### Pipeline as of 2026-09-18 (all normal, nothing stuck)

| Status | Amount | Project | Expected |
|---|---|---|---|
| OPEN | 14,667.30 | Nasima Majid activation (approved Thu 09-17) | Tue 2026-09-22 |
| PAUSED | 3,870.00 | Nasima Majid DC holdback | Thu 2026-10-15 |
| PAUSED | 2,666.00 | Justin Kane DC holdback | per +23d rule |
| PAUSED | 2,236.00 | Mustaque Nabi DC holdback | per +23d rule |
| PAUSED | 1,720.00 | Ryan Hopley DC holdback | Thu 2026-10-08 |

A non-zero `amount_remaining` on an in-flight project is also normal -- it is
just the unpaid portion. Mohammed Alamgir | 28 (9,137.50) and Kim & James
Murphy (12,848.40) are each EXACTLY 20% of contract value, i.e. activations not
yet paid. Do not treat `amount_remaining > 0` as an exception.

**What IS worth alerting on:** a milestone sitting in `rejected` (see
`LightReach_MilestoneLog`), or an approval whose predicted batch date has
passed with nothing batched against it.

### Clawbacks: event date != cash date (corrected 2026-09-18)

A clawback is only applied to cash once it is APPROVED and assigned a batch.
The `event_datetime` is when LightReach's system triggered it, which can precede
the batch by a long way and means nothing financially.

Ghazal, read correctly:
- clawback event Jul 4, **batch 09-10**
- true-up event Sep 10, **batch 09-15**

So the money was out for about **five days**, not the five months an
event-date reading suggests.

**Rule: only batch dates move money.** A row with no batch date has not
affected cash, whatever its status (`OPEN`, `PAUSED`, or a triggered but
unapproved clawback). Build every cash calculation off `batch`, and treat
`event_datetime` purely as the approval trigger for predicting a future batch.

This also explains the single +63d outlier in the batch-rule test -- that was
the Ghazal clawback, where the trigger-to-batch gap is not governed by the
Tue/Thu rule at all.

### Clawback extension requests -- flag only, do NOT model

Helio can often submit a clawback extension request, in which case the clawback
shows in the ledger but is never approved and never batches, so no cash moves.
A triggered clawback is therefore an ACTION ITEM, not a cash event.

Harry (2026-09-18): the rules behind clawback triggers are tricky -- **do not
build logic around them yet.**

Engine behaviour:
- clawback row present with no batch date -> surface it with the amount at risk,
  once, and nothing more
- do NOT predict clawbacks, infer deadlines, or model the trigger conditions
- do NOT book the cash unless and until a batch date appears

## Subcontractor cost classification -- three defects (2026-09-18)

`_get_commission_data()` in `main.py` (~line 3890) builds `subcontractor_total`
by scanning `Adder_Details_JSON` for names starting `D. MISC:`, minus a
hardcoded keyword deny-list. Three problems.

### Defect 1: only `D. MISC:` is scanned, so trenching is never counted

The adder vocabulary has four prefixes: `A -` / `A.` (comp, referral, equip),
`B. LABOR:`, `C. ELECTRICAL:`, `D. MISC:`. Only the last is examined.

**Harry confirmed trenching IS a subcontractor cost**, and every trenching adder
lives under `B. LABOR:`:
- `B. LABOR: Trenching HARD SURFACE  ($84 per Linear Foot)`
- `B. LABOR: Trenching GRASS ($42 per Linear Foot))`
- `B. LABOR: Trenching GROUNDMOUNT (Custom Priced by Helio)`
- `B. LABOR: Trenching for Ground Mount ($42 /Lineal Foot)`
- `B. LABOR: Custom Trenching (Concrete / Pavers)`

These currently forecast as $0 cash out on every project that has them.

### Defect 2: classification is a deny-list, not an explicit map

Replace the `INTERNAL_ADDER_KEYWORDS` substring check with an explicit
name -> {internal | subcontractor} table. Anything NOT in the table must be
flagged for review, never silently assumed internal or external.

Confirmed classifications (Harry, 2026-09-18):

| Adder | Classification |
|---|---|
| `D. MISC: Roof Replacement (Outside Contractor)` | SUBCONTRACTOR |
| `D. MISC: Roof Replacement (GAF Roofing)` | SUBCONTRACTOR |
| `D. MISC: Roof Replacement (Owens Corning Preferred 50-Yr Warranty)` | SUBCONTRACTOR |
| `D. MISC: Tree Removal / Trimming` | SUBCONTRACTOR |
| `B. LABOR: Trenching *` (all variants) | SUBCONTRACTOR |
| `D. MISC: Custom Electrical Work Needed` | INTERNAL -- always done in house |
| `D. MISC: Roof Replacement (INTERNAL HGC ROOFING)` | INTERNAL |
| `D. MISC: Removal of Existing PV System` | INTERNAL |

Also INTERNAL (Harry, 2026-09-18): `B. LABOR: Ground Mount (>6kW)`,
`B. LABOR: EV Charger Level 2`, and all `C. ELECTRICAL:` panel upgrades
(200A Main Service, 125A/200A Main Panel Replacement, 200A SPAN Smart Panel,
LUMIN Smart Load Management). All other `B. LABOR:` items (High Pitch,
High Roof, Ballasted Flat Roof, Over 3 Arrays, Small System Upcharge,
Relocate Roof Vent Pipe) are in-house crew labour -- internal.

So the ONLY subcontractor costs are: roofing by an outside contractor,
tree removal/trimming, and trenching. Everything else is internal.

Note `D. MISC: Custom Electrical Work Needed` carries real money ($1,500-$5,000
per project) and is common -- excluding it is correct but consequential, so it
should be visible in the sheet as an internal cost, not invisible.

### Defect 3: ~1 in 5 active projects has no adder data at all

`subcontractor_total` comes from the install's `Active_Snapshot`. No snapshot
means no adders, no sub cost, and no Aurora commission data -- the forecast
shows zero rather than unknown.

Of 49 active (non-cancelled, non-closeout) Installs, **9 have no
`Active_Snapshot`**: Daniel Ghazal, Alex Starr | Commercial, GVFC ASnell,
Ramsey Goodrich, Michael Duke, Richard Desrochers, Daniel DeTuccio,
Frank & Josephine LaFauci, Bob Muller.

Five of those DO have an `Aurora_Project_ID` (Ghazal, Duke, Desrochers,
DeTuccio, Muller) -- the snapshot simply was never created, and `main.py`
already has machinery for this case (see `_create_initial_snapshot_for_install`
and the backfill job around line 1062). The other four have no Aurora project.

Across all non-cancelled Installs: 63 with a snapshot, 375 without (most of the
tail is historical and outside the cashflow window).

### Snapshot backfill results (2026-09-18)

Ran `/internal/create-initial-snapshot` for all five recoverable installs:

- **Ghazal, Duke, Desrochers, DeTuccio -- still blocked, and it's an Aurora
  data gap, not a code bug.** All four have zero Aurora designs tagged
  `milestone: sold` (`_create_initial_snapshot_for_install` requires exactly
  one). Nothing to fix here; they correctly stay in the Defect 3 "no adder
  data" bucket until a design gets marked sold in Aurora.
- **Bob Muller -- fixed.** Failed with `INVALID_DATA`, `api_name:
  "Incentives_Total"`, `maximum_length: 16`. Root cause: `extract_pricing_fields`
  set `Incentives_Total`/`Solar_Incentives_Total`/`Storage_Incentives_Total`/
  the `*_Before_Incentives` fields from raw unrounded float arithmetic --
  `solar_incentives_total + storage_incentives_total` can produce a value
  like `34999.999999999996` (18 chars), over Zoho's 16-char field limit,
  where every other pricing field in the same function was already
  `round(..., 2)` (see `Final_System_Price` a few lines up). Fixed by
  rounding all six fields to 2 decimals. Retried after deploy: snapshot
  `5264387000097199003` created successfully.

So of the original 9 no-snapshot installs: Muller now has one, 4 (Ghazal/
Duke/Desrochers/DeTuccio) correctly remain snapshot-less pending an Aurora-side
fix, and the other 4 (Alex Starr, GVFC ASnell, Ramsey Goodrich, LaFauci) have
no Aurora project at all and are expected to stay that way.

### Required fixes

1. Extend the classifier to all prefixes, driven by the explicit table above.
2. Backfill the five recoverable snapshots; investigate why creation was missed.
3. **Never forecast $0 sub cost for a project with no snapshot.** Emit a
   distinct "no adder data" state so the sheet shows unknown, not zero.
   Same principle as the materials tiers: a gap must look like a gap.

## Available cash vs forecast -- a second, separate problem (2026-09-18)

Harry's actual goal for the forecast is "how much cash will we have each week."
But there is a distinct question the 17-week forecast cannot answer:

> Of the money sitting in the account **today**, how much is genuinely ours?

That is not a forecasting problem, it is a commitments ledger:

```
Chase balance
  - customer deposits held against work not yet delivered
  - materials / sub costs incurred but unpaid
  - commissions earned but not yet paid out
  - accrued payroll and taxes
= available
```

Recalculated daily. Shares inputs with the forecast but answers a different
question: the forecast says whether payroll clears in November; this says
whether $40k can be spent this week.

### What we established

- **Cash deals are invoiced through QuickBooks Online.** No QB connector exists
  in the MCP registry, but QuickBooks Online IS reachable via the Zapier
  connector (`QuickBooksV3CLIAPI`, 29 read actions).
- **The 20 / 60 / 20 milestones are SEPARATE invoices in QB.** This is what
  makes "collected against undelivered work" exactly computable rather than
  estimated -- a paid deposit and a paid pre-install invoice on a project that
  has not been installed are money held, not earned.
- **Chase feeds into QuickBooks**, but reconciliation is behind. Harry: the
  blocker is **ownership**, not tooling. An available-cash view built on an
  unreconciled ledger would be confidently wrong, so reconciliation currency is
  a hard prerequisite.
- **Payment mix:** mostly check and ACH. Credit cards go through **Heartland**
  (a separate processor) and are **at most 5%** -- batched, net of fees, hardest
  to attribute. Do not build for Heartland; handle by hand.
- LightReach removes most deposit risk: the lender pays after milestones, and
  materials go direct to the vendor and never touch the account. **Deposit
  exposure is concentrated in cash deals.**

### Why this is now tractable

Every LightReach deposit can be identified **exactly** -- the funding ledger
gives the amount and the batch date (e.g. $7,447.60 on 2026-09-17). At ~80% of
jobs, a large share of unmatched Chase deposits can be auto-attributed to a
named project with no human lookup. Cash-deal check/ACH deposits can be
proposed against QB open invoices, whose amounts are distinctive (20% of a
contract).

This reframes the ownership blocker: reconciliation is avoided because it is a
tedious multi-hour job. If most deposits arrive pre-matched and what remains is
a short approve-or-correct list, it becomes a ~10-minute weekly task. Automation
does not assign ownership but can shrink the job until ownership stops being
the bottleneck.

### Sequencing (not started)

1. Auto-match LightReach deposits from the funding ledger -- data already exists
   in `artifacts/lr_funding_*.csv`.
2. Propose matches for check/ACH against QB open invoices.
3. Only then build the available-cash view, once the ledger is current.

### Caveat

The treatment of customer deposits as a liability is an accounting question for
Janelle, not something to decide here. Build the operational view; have her
confirm it ties to the books.

## Chase via SimpleFIN + Palmetto deposit matching (2026-09-18)

### SimpleFIN access

```
op://Helio Automation/SimpleFIN/SIMPLEFIN_ACCESS_URL
op://Helio Automation/SimpleFIN/SIMPLEFIN_ACCOUNT_IDS
```

The access URL is `scheme://user:pass@host/path` with credentials embedded --
it IS the credential. Read-only by protocol; SimpleFIN cannot write to a bank.
`GET {ACCESS_URL}/accounts`, max 90-day window per request. Transaction fields:
`amount, description, id, mcc, memo, payee, posted, transacted_at`.

**SimpleFIN exposes 20 accounts, including Harry's PERSONAL ones** (SoFi, Apple
Card, LightStream). `SIMPLEFIN_ACCOUNT_IDS` is an explicit ALLOWLIST and code
must refuse to run without it, filtering BEFORE reading transactions. A newly
linked personal account must be excluded by default, never included by default.

Never print balances, amounts, descriptions or payees for non-LightReach
transactions into CI logs.

### The two Chase accounts

| Account | Role |
|---|---|
| PERFBUS CHK (1030) | main operating account |
| PERFBUS CHK (1055) | money deliberately set aside so it is not spent from 1030 |

1055 is already a manual restricted-cash mechanism. Any "available cash" view
must account for it or it will double-count.

### LightReach pays in BATCHES, from Palmetto

Confirmed empirically (90-day window, 60 ledger rows, 19 batch dates,
19 Palmetto deposits):

- per-project deposits: **2 of 60 matched (3%)** -- not how it works
- batch-total deposits: **13 of 19 matched (68%), and those 13 matched EXACTLY**

Payee string is consistent: **`Certificate of Origin Palmetto Solar L`**.
Filter on `palmetto` in payee+description to isolate LightReach deposits.

So: one Chase deposit per batch date = sum of every project paid in that batch.
When the ledger is complete the totals reconcile to the penny.

### The 6 unmatched batches are OUR gap, not LightReach's

Every gap is positive -- the deposit exceeded the sum of the projects we knew
about:

| Batch | Expected | Actual | Gap |
|---|---|---|---|
| 2026-07-14 | 23,466.82 | 28,819.78 | +5,352.96 |
| 2026-08-13 | 15,304.56 | 22,229.28 | +6,924.72 |
| 2026-08-18 | 32,080.91 | 39,738.35 | +7,657.44 |
| 2026-09-10 | -3,388.74 | 567.26 | +3,956.00 |

**Root cause:** `lightreach_funding_audit.py` enumerates accounts from Zoho's
`LightReach_Account_ID`. Any LightReach project whose Zoho record lacks that
field was never scraped, so its payout is missing from every batch sum it
belongs to.

**Fix: drive the scrape from Palmetto's own Accounts list, not from Zoho.**
That captures every account LightReach has paid on, and as a side effect finds
projects that exist in Palmetto but are missing or unlinked in the CRM.

### Clawbacks net inside a batch

Batch 2026-09-10 summed to **-3,388.74** in the ledger (Ghazal's $48,048
clawback) yet the Chase deposit was **+567.26**. So a clawback is absorbed
within the batch rather than debited separately, and a batch can still land
positive. Model it as a negative line inside the batch total.

### Why this matters

19 Palmetto deposits in 90 days, one payee string, exact batch reconciliation.
Once the ledger is complete, every LightReach deposit can be decomposed into
named projects automatically -- no QuickBooks involvement required. That is the
bulk of the reconciliation backlog.

## Adder classification -- corrections (2026-09-18, supersedes the table above)

**The prefix does NOT predict whether an adder is subcontracted.** Critter Guard
is `A. EQUIP:` and IS a subcontractor cost. Classification must be per-adder,
from Harry, never inferred from the category prefix.

SUBCONTRACTOR (cash out to an outside party):
- `D. MISC: Roof Replacement (*)` -- EXCEPT the INTERNAL HGC variant
- `D. MISC: Tree Removal / Trimming`
- `B. LABOR: *Trenching*` -- all variants
- `A. EQUIP: Critter Guard`

INTERNAL (no outside cash out):
- everything else, including `D. MISC: Custom Electrical Work Needed`,
  `D. MISC: Removal of Existing PV System`,
  `D. MISC: Roof Replacement (INTERNAL HGC ROOFING)`,
  `B. LABOR: Ground Mount (>6kW)`, `B. LABOR: EV Charger Level 2`,
  all `C. ELECTRICAL:` panel upgrades, and all other `B. LABOR:` crew items.

**Open, not critical:** `B. LABOR: Over 3 Arrays ($300 per Array)` is INTERNAL
for now. Harry wants to revisit it later.

**Still worth doing:** there are only ~25 distinct adder names in the whole
book. Walk the full distinct list with Harry once and mark each explicitly,
rather than discovering misclassifications project by project.

Implement as an ALLOWLIST of subcontractor patterns -- internal by default,
unknown names flagged for review and counted in neither bucket.

### Correction: ALL roof replacement is subcontracted (2026-09-18)

Harry: "roof is not internal." `D. MISC: Roof Replacement (INTERNAL HGC ROOFING)`
is a SUBCONTRACTOR cost like every other roof replacement variant. The original
`INTERNAL_ADDER_KEYWORDS` deny-list in `main.py` was wrong on this item, not
merely incomplete.

Final subcontractor allowlist (four patterns, no exceptions):
- `D. MISC: Roof Replacement (*)`  -- ALL variants incl. INTERNAL HGC
- `D. MISC: Tree Removal / Trimming`
- `B. LABOR: *Trenching*`          -- all variants
- `A. EQUIP: Critter Guard`

Everything else internal by default; unknown names flagged, never absorbed.

## Money-out reconciliation: bills vs. the cashflow forecast (2026-09-18)

Mirrors the money-in problem above (LightReach batch matching, QB invoice
matching for cash deals), but for the other side of the ledger: did the bills
the cashflow file said were coming due actually get paid?

### The shape of it

`cashflow.py`'s Pipeline tab already generates expected outflows with dates --
commissions, subcontractor payments, CT Green Estates, materials. Same
plumbing as the deposit-matching work (SimpleFIN pull over a window, propose
matches against a known list), applied to debits instead of credits:

For each expected outflow whose date has passed, look for a matching Chase
debit by amount and rough date. Three outcomes:

1. **Matched** -- confirmed paid, close it out.
2. **No match** -- either it hasn't gone out yet, it was forecast wrong, or
   someone forgot. Needs a human look, not a silent assumption either way.
3. **Chase debit with no forecast match at all** -- an outflow the cashflow
   file never knew about. This is the one worth watching: it catches a
   surprise expense before it becomes a cash problem instead of after.

### Sequencing

This is a natural next build on the same SimpleFIN plumbing, but sits behind
the money-in work already queued above:

1. Subcontractor cost classification fixes (three defects, 2026-09-18 section
   above) -- the Pipeline tab's outflow amounts depend on this being correct.
2. Palmetto ledger completeness fix (scrape from Palmetto's Accounts list, not
   Zoho's `LightReach_Account_ID`) -- needed so the money-in side of the
   ledger is verified end to end before adding a money-out reconciliation on
   top of it.
3. Only then: build the bills-vs-outflows matcher described above.

### Caveat

Same as the available-cash caveat: this produces an operational match/flag
list, not an accounting determination. A debit with no forecast match is a
flag for Harry to explain, not evidence on its own that something was missed.

## Palmetto Accounts list: the real backing API (found 2026-09-18)

The "Accounts" nav item is a Kanban-style board over a JSON API, not a plain
HTML table -- found by capturing network traffic during page load:

```
GET https://palmetto.finance/api/accounts/summary
  ?currentMilestone=undefined&pageNum=<n>&advancedFilters=%5B%5D
  &searchTerm=undefined&includeCompletedAccounts=<bool>
  &onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST
```

Auth is via the browser session cookie set at login -- `page.request.get(url)`
inside the same Playwright context works directly, no token wrangling needed.

Response shape: `{total, pageNum, data: {accounts: [...], milestoneSummary}}`.
20 accounts per page. `accounts[].currentMilestone.type` is one of
`initialAccountDetails` (Qualification), `noticeToProceed`, `install`,
`activation`.

**`includeCompletedAccounts=false` (the default the UI's own nav uses) hides
every completed/funded account.** Confirmed by toggling it: total went from
1286 to 1373, and the entire +87 delta landed in the `activation` milestone
count (3 -> 90). A completed account never leaves the `activation` milestone
type -- it just carries a different `status` on it. So the full historical
LightReach book only surfaces with this flag set to `true`; the visible nav
counts (Qualification 1 / Notice to Proceed 1265 / Install 17 / Activation 3)
are the in-progress-only default view, not the whole universe.

**1265 of 1286 default-view accounts sit at Notice to Proceed.** Harry
confirmed these are mostly pre-contract leads, not funded projects -- nothing
to scrape on a /funding page. Only `install` and `activation` milestone
accounts are worth scraping (`FUNDED_MILESTONE_TYPES` in
`lightreach_funding_audit.py`).

**`externalReference` on an account is NOT the Aurora Project ID** -- checked
directly against a known Zoho record (Abdul Mabud: externalReference
`5f30be1b-...` vs Aurora_Project_ID `19c7d101-...`, different UUIDs). Some
other system's reference, unconfirmed what. Don't use it as a join key.

**The Zoho join is address-based**, reusing `normalize_street`/
`normalize_city`/`split_site_location` from `powerclerk_sync.py` (the same
messy-free-text-address problem PowerClerk matching already solved) --
`LightReach_Account_ID` exact match first, address fallback second, marked
unmatched (not dropped) otherwise.

### Pagination drift under sort=NEWEST (found and fixed 2026-09-18)

The first full walk (paging every one of 1373 accounts to filter down to
Install/Activation client-side) took several minutes, longer with 429
backoffs, while `sort=NEWEST` is not a stable key over that span -- the
underlying list keeps changing in near-real-time. A same-day investigation
run found 4 accounts genuinely at `currentMilestone.type=install` (three
Adrian Scarpa variants, Nilo Torres -- all `status: paused`) that the full
walk's 107-account result had silently missed.

**Fix:** `fetch_palmetto_accounts()` tries the API's own `currentMilestone=`
query param as a real server-side filter first -- confirmed it works
(`/api/accounts/summary honors currentMilestone=`) -- so it does one short,
focused walk per funded milestone type instead of one long walk of
everything. Only falls back to the full walk (now deduped by id, stopping
on two consecutive empty pages rather than trusting the reported `total`)
if the server doesn't actually honor the filter.

Even after this fix, the *same* 107 accounts came back on a re-run
(paused/cancelled accounts like the Scarpa ones stay excluded by the list
endpoint even under a direct milestone match, unlike a direct per-id GET)
-- so those 4 are not a coverage bug in the current code, just accounts
that no longer show up in ANY list view. Still unconfirmed whether they
carry historical ledger activity relevant to the 90-day reconciliation
window; flagged below, not resolved.

### Part 2 result: 19/19 (2026-09-21) -- fixed

Two full runs after the Palmetto-driven fix (2026-09-18), ~40 minutes apart,
gave inconsistent results:

| Run | Ledger rows in window | Batch dates | Matched |
|---|---|---|---|
| 1 | 64 | 19 | 16 (84%) |
| 2 | 57 | 18 | 12 (67%) |

The **set of which batches match changed entirely between the two runs**,
pointing at scrape completeness noise in `scrape_account()`'s per-account
ledger-table read rather than a remaining enumeration gap.

**Root cause, confirmed 2026-09-21:** the 15s wait for `table tbody tr`
swallowed its own timeout and just proceeded with whatever `extract_ledger`
found (often 0 rows) -- a slow-but-real render was indistinguishable from a
genuinely empty ledger. Turned out to be systemic, not occasional: on the
run after the fix, the majority of accounts hit the 15s timeout on their
*first* attempt and needed a retry.

**Fix:** `scrape_account()` now treats a ledger-table timeout as a retriable
failure (re-navigates and tries the whole page again, default retries
raised 2 -> 3), and also retries once when the wait succeeded but the
ledger still came back with 0 rows on an account whose payment plan shows
real approved amounts. Only accepted as genuinely empty after exhausting
retries.

**Result:** 344 ledger rows (up from 291), 68 in the 90-day window (up from
57-64) across all 19 batch dates, **19/19 batch-total deposits matched
(100%)**. 107 ok / 0 skipped / 0 failed.

## Chase bank reconciliation -> Cashflow Revenue/Expenses (2026-09-21)

Goal: reconcile real Chase transactions against the Cashflow sheet's Revenue
(customer payments) and Expenses (commissions, subcontractor, materials, CT
Green Estates, recurring bills) tabs, both directions, so the forecast can be
marked confirmed instead of only ever showing a guess. Explicitly does NOT
try to re-solve LightReach batch matching -- that's `lr_deposit_match.py`,
already at 19/19 (previous section). A Chase transaction whose payee
contains "palmetto" is labeled as a LightReach batch and left unmatched by
this system rather than treated as a mystery.

### Why two systems, not one

SimpleFIN credentials stay CI-only (never added to Render) and Render's
Google Sheets credentials stay Render-only (never added to CI) -- same
security posture as the rest of this repo's SimpleFIN work. The propose step
needs both bank data AND current sheet state, so it's split:

- **`scripts/bank_reconciliation_propose.py`** (GitHub Actions,
  `.github/workflows/bank-reconciliation.yml`, cron Mon/Wed/Fri 12:00 UTC +
  `workflow_dispatch`): pulls Chase transactions for BOTH Helio accounts
  (1030 and 1055 -- 1055 is a full peer here, not an afterthought, since
  materials payments sometimes come out of it), reads current match
  candidates from Render (`GET /internal/cashflow-snapshot`), computes
  matches (exact amount, within a `[week_monday - 3d, week_monday + 10d]`
  window), and POSTs the results to Render
  (`POST /internal/cashflow-reconcile-write-proposals`).
- **Render (main.py)**: never touches SimpleFIN. Owns the actual Sheets
  read/write for the Reconciliation tab (new) and for stamping Paid/Received
  on Expenses/Revenue rows.

### New Reconciliation tab

Created by `_ensure_reconciliation_tab` (mirrors `_ensure_overrides_tab`
exactly). Columns: Date, Amount, Account, Direction, Payee, Proposed Match,
Match Basis, Match Key (internal -- `EXP|week_serial|category|name` or
`REV|week_serial|category|name`, used by Apply), Manual Category (blank;
fill in "Expense" or "Revenue" to turn an UNMATCHED row into a new manual
entry on Apply), Notes, Approved (checkbox, Harry sets this), Applied
(script-managed), Applied Date.

Re-run-safe by design: a Chase transaction is keyed by (date, amount,
account). Seeing it again only refreshes Proposed Match/Basis/Key; Approved/
Applied/Manual Category/Notes are never touched by a re-run, and a row
already `Applied=TRUE` is completely frozen (immutable historical record,
same principle as a Paid project-expense row).

### Revenue tab gained a status column

`_write_dashboard_revenue_tab` used to write only `A:Week B:Category
C:Amount D:Project E:Notes`. Added `F:Received G:Received Date`, preserved
across every rewrite by `_read_revenue_received_keys` /
`_restore_revenue_received_statuses` -- new functions, directly modeled on
the Expenses tab's existing `_read_expense_paid_keys` /
`_restore_expense_paid_statuses`, same `(week_serial, category, name)` key
shape.

### Apply: `POST /cashflow/reconcile-apply`

No bank credentials needed. Reads the Reconciliation tab; for every row with
`Approved=TRUE` and `Applied` not TRUE: stamps the matched Expenses row's
Status=Paid or the matched Revenue row's Received=TRUE (by Match Key), OR --
if there's no Match Key but Manual Category is filled in -- appends a new
manual row to Expenses/Revenue and marks it Paid/Received immediately (the
"catch a surprise expense/deposit" path). Always stamps `Applied=TRUE` +
today's date on the Reconciliation row when it does either.

Safe to trigger anytime, including from inside the spreadsheet itself.

### The Cashflow sheet already has its own Apps Script -- not in this repo

Discovered 2026-09-21, the hard way (my first instruction was to paste over
it): the Cashflow spreadsheet has a pre-existing bound Apps Script project
with a "Helio" menu -- Run Cash Flow, Apply Overrides, Sync Expenses, Sync
Submissions, Rebuild Dashboard Structure -- calling `/cashflow/run`,
`/dashboard/apply-overrides`, `/dashboard/sync-expenses`,
`/dashboard/sync-submissions`, `/dashboard/create` in `main.py`. It is NOT
tracked in this git repo; it only exists in the spreadsheet itself
(Extensions -> Apps Script from the sheet). A `const BASE_URL` and
`callEndpoint(path, method)` helper already exist there -- reuse them, don't
redefine.

`apps_script/cashflow_reconciliation_menu.gs` in this repo documents the two
small ADDITIONS needed in that existing script (one line in `onOpen()`, one
new function appended) -- it is reference material, not a file meant to be
pasted in wholesale. Any future Apps Script change to the Cashflow sheet
must edit-in-place there, never overwrite.

### Trigger from the Cashflow sheet

Two menu items folded into the existing "Helio" menu (not a separate
top-level one):

- **"Apply Approved Reconciliation Matches"** -- calls
  `/cashflow/reconcile-apply` directly via `UrlFetchApp.fetch`. No bank
  creds involved, safe as a one-click action.
- **"Pull New Chase Transactions"** -- triggers the `bank-reconciliation.yml`
  workflow_dispatch via GitHub's REST API, so Harry doesn't have to wait for
  the Mon/Wed/Fri cron. Needs a GitHub fine-grained PAT (scoped to just this
  repo, Actions: Read and write) stored in the Apps Script project's Script
  Properties under `GITHUB_PAT` -- a one-time setup Harry did himself (Claude
  cannot create a PAT; GitHub has no API for that). The token never appears
  in the script body or in this repo.

### Status: built and confirmed working end-to-end (2026-09-21)

First real run: 304 Chase transactions pulled (both 1030 and 1055, last 45
days), 4 matched to Revenue, 26 to Expenses, 11 labeled as LightReach
batches (left for `lr_deposit_match.py`, not matched here), 263 unmatched --
all written as new Reconciliation rows. `/cashflow/reconcile-apply` verified
as a safe no-op with nothing yet approved. Not yet approved/applied against
real data -- next step is spot-checking a handful of the 30 matched rows by
hand before checking any Approved boxes for real.
