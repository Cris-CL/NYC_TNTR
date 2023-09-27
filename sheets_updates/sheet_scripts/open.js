function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('Update')
    .addItem('URI', 'openDialog')
    .addItem('会計詳細データ一覧 > セット毎', 'openDialog_2')
    .addToUi();
}

function openDialog() {
  var html = HtmlService.createHtmlOutputFromFile('InputDialog')
    .setWidth(400)
    .setHeight(200);
  SpreadsheetApp.getUi().showModalDialog(html, 'Input Data');
}

function openDialog_2() {
  var html = HtmlService.createHtmlOutputFromFile('shosai_data')
    .setWidth(400)
    .setHeight(200);
  SpreadsheetApp.getUi().showModalDialog(html, 'Input Data');
}

function sendPostRequest(column=1,date=0,type_name) {
  // Replace 'YOUR_URL_HERE' with the actual URL you want to send the request to
  var url = 'CHANGE_FOR_YOUR_URL';

  Logger.log(column);
  Logger.log(date);
  // Check if the month is a valid integer between 1 and 12
  // date = parseInt(date);
  // Create the request payload
  var payload = {
    column: column,
    date: date,
    type:type_name
  };

  // Create the options for the HTTP request
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload)
  };

  // Send the POST request
  UrlFetchApp.fetch(url, options);
  // Inform the user that the request has been sent
  Browser.msgBox('POST request sent successfully.');
  }
