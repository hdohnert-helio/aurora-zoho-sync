/**
 * NOT a standalone file to paste in wholesale -- the Cashflow sheet already
 * has its own Apps Script project (a "Helio" menu: Run Cash Flow, Apply
 * Overrides, Sync Expenses, Sync Submissions, Rebuild Dashboard Structure,
 * calling /cashflow/run, /dashboard/apply-overrides, /dashboard/sync-expenses,
 * /dashboard/sync-submissions, /dashboard/create in main.py). That script
 * isn't tracked in this repo -- it only lives in the spreadsheet's bound
 * Apps Script project (Extensions -> Apps Script from the sheet itself).
 *
 * This file documents the additions needed there, for reference -- don't
 * paste this file over the existing Code.gs.
 *
 * 1. Add these lines to the existing onOpen(), right before .addToUi():
 *
 *      .addSeparator()
 *      .addItem("Apply Approved Reconciliation Matches", "applyApprovedMatches")
 *      .addItem("Pull New Chase Transactions", "runProposeNow")
 *
 * 2. Append both functions below to the bottom of the existing Code.gs
 *    (applyApprovedMatches reuses the file's existing BASE_URL constant and
 *    callEndpoint() helper -- no new constant needed).
 *
 * 3. runProposeNow() additionally needs a GitHub fine-grained PAT (repo
 *    scoped to aurora-zoho-sync only, Actions: Read and write, created at
 *    https://github.com/settings/personal-access-tokens/new -- Claude
 *    cannot create this, GitHub doesn't allow API-based token creation)
 *    stored in the Apps Script project's Script Properties (gear icon ->
 *    Project Settings -> Script Properties) under the key GITHUB_PAT. Never
 *    hardcode the token in the script body.
 */

function applyApprovedMatches() {
  const ui = SpreadsheetApp.getUi();
  const confirm = ui.alert(
    "Apply Approved Matches",
    "This will stamp Paid/Received on every Reconciliation row you've checked Approved on. Continue?",
    ui.ButtonSet.YES_NO
  );
  if (confirm !== ui.Button.YES) return;

  const result = callEndpoint("/cashflow/reconcile-apply");
  if (result.status === "ok") {
    ui.alert(
      "✓ Reconciliation applied\n" +
      "Applied: " + result.applied + "\n" +
      "New manual rows: " + result.new_manual_rows + "\n" +
      "Skipped: " + result.skipped
    );
  } else {
    ui.alert("Error: " + (result.error || result.detail || JSON.stringify(result)));
  }
}

function runProposeNow() {
  const ui = SpreadsheetApp.getUi();
  const token = PropertiesService.getScriptProperties().getProperty("GITHUB_PAT");
  if (!token) {
    ui.alert("No GitHub token set. Project Settings > Script Properties > add GITHUB_PAT.");
    return;
  }
  const confirm = ui.alert(
    "Pull New Chase Transactions",
    "Kicks off a fresh Chase pull and proposes new matches (takes about a minute, runs in the background -- check the Reconciliation tab shortly after). Continue?",
    ui.ButtonSet.YES_NO
  );
  if (confirm !== ui.Button.YES) return;

  const response = UrlFetchApp.fetch(
    "https://api.github.com/repos/hdohnert-helio/aurora-zoho-sync/actions/workflows/bank-reconciliation.yml/dispatches",
    {
      method: "post",
      contentType: "application/json",
      headers: {
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json"
      },
      payload: JSON.stringify({ ref: "main" }),
      muteHttpExceptions: true
    }
  );

  if (response.getResponseCode() === 204) {
    ui.alert("✓ Started -- check the Reconciliation tab in about a minute.");
  } else {
    ui.alert("Error (" + response.getResponseCode() + "): " + response.getContentText());
  }
}
