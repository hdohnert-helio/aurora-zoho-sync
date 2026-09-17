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

True when the portal status contains any of: `Corrections Required`,
`Customer Action Required`, `InComplete`, `Unsubmitted`, `On Hold`,
`Awaiting Meter Fee`. False for `Re-Review` (utility is working).
Also true when no portal application is found at all for an active project.

### Sync rules (non-negotiable)

1. NEVER overwrite a non-empty `IC_Project_Number`. Fill blanks only; on conflict,
   record it in `IC_Portal_Status` and flag -- do not silently change.
2. Notify only on CHANGE (false->true on `IC_Action_Required`, or status string
   change). A daily restatement of unchanged blockers gets ignored within a week.
3. Always stamp `IC_Portal_Checked`, including on failure -- a stale timestamp is
   the only signal that the sync itself broke.
4. All Zoho writes send `"trigger": []` and skip `assignment_rules` /
   `connected_workflows`. Note: `skip_feature_execution` accepts max 2 entries.
5. Schedule off-hours (05:00 ET / 09:00 UTC). Each run evicts whoever is logged
   into PowerClerk -- one session per account.

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
