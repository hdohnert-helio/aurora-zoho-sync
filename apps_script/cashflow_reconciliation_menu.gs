/**
 * Adds a "Reconciliation" menu to the Cashflow Google Sheet with one item:
 * "Apply Approved Matches Now" -- calls Render's /cashflow/reconcile-apply.
 *
 * This needs no bank credentials (Apply never touches SimpleFIN -- it only
 * reads the Reconciliation tab and writes Revenue/Expenses), so it's safe
 * to expose as a one-click menu action.
 *
 * Setup (one-time, done by a human in the Sheets UI, not by Claude):
 *   1. Open the Cashflow Google Sheet.
 *   2. Extensions -> Apps Script.
 *   3. Delete any placeholder code in Code.gs, paste this file's contents.
 *   4. Save (the project name doesn't matter).
 *   5. Reload the spreadsheet tab. A "Reconciliation" menu appears next to
 *      Help. The first click will prompt for authorization (this script
 *      calls an external URL) -- approve it once.
 */

var RENDER_BASE_URL = 'https://aurora-zoho-sync.onrender.com';

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Reconciliation')
    .addItem('Apply Approved Matches Now', 'applyApprovedMatches')
    .addToUi();
}

function applyApprovedMatches() {
  var ui = SpreadsheetApp.getUi();
  var response = ui.alert(
    'Apply Approved Matches',
    'This will stamp Paid/Received on every Reconciliation row you\'ve checked Approved on, and is not reversible from this menu. Continue?',
    ui.ButtonSet.YES_NO
  );
  if (response != ui.Button.YES) {
    return;
  }

  var result;
  try {
    var resp = UrlFetchApp.fetch(RENDER_BASE_URL + '/cashflow/reconcile-apply', {
      method: 'post',
      muteHttpExceptions: true,
      // Render can take a little while to wake from idle on the free tier;
      // Apps Script's own fetch timeout is fixed (~30-60s), which is usually
      // enough, but a timeout here just means "check the Reconciliation tab
      // again in a minute" rather than a real failure.
    });
    result = JSON.parse(resp.getContentText());
  } catch (e) {
    ui.alert('Apply failed to reach the service: ' + e.message);
    return;
  }

  if (result.status !== 'ok') {
    ui.alert('Apply failed: ' + (result.detail || result.reason || JSON.stringify(result)));
    return;
  }

  ui.alert(
    'Done',
    'Applied: ' + result.applied +
      '\nNew manual rows created: ' + result.new_manual_rows +
      '\nSkipped (no longer matchable): ' + result.skipped,
    ui.ButtonSet.OK
  );
}
