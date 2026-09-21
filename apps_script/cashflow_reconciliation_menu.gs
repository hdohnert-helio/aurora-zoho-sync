/**
 * NOT a standalone file to paste in wholesale -- the Cashflow sheet already
 * has its own Apps Script project (a "Helio" menu: Run Cash Flow, Apply
 * Overrides, Sync Expenses, Sync Submissions, Rebuild Dashboard Structure,
 * calling /cashflow/run, /dashboard/apply-overrides, /dashboard/sync-expenses,
 * /dashboard/sync-submissions, /dashboard/create in main.py). That script
 * isn't tracked in this repo -- it only lives in the spreadsheet's bound
 * Apps Script project (Extensions -> Apps Script from the sheet itself).
 *
 * This file documents the two additions needed there, for reference --
 * don't paste this file over the existing Code.gs.
 *
 * 1. Add one line to the existing onOpen(), right before .addToUi():
 *
 *      .addSeparator()
 *      .addItem("Apply Approved Reconciliation Matches", "applyApprovedMatches")
 *
 * 2. Append this function to the bottom of the existing Code.gs (reuses the
 *    file's existing BASE_URL constant and callEndpoint() helper -- no new
 *    constant needed):
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
