
// function myFunction() {

// }
function sendPostRequest() {
  // Replace 'YOUR_URL_HERE' with the actual URL you want to send the request to
  var url = 'https://us-central1-test-bigquery-cc.cloudfunctions.net/hostess_salary';

  // Prompt the user for input (name and month)
  var name = Browser.inputBox('Enter a name:');
  var month = Browser.inputBox('Enter a month (1-12):');

  // Check if the month is a valid integer between 1 and 12
  if (isInteger(month)) {
    month = parseInt(month);
    if (month >= 1 && month <= 12) {
      // Create the request payload
      var payload = {
        name: name,
        month: month
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
    } else {
      // Invalid month
      Browser.msgBox('Invalid month. Please enter a number between 1 and 12.');
    }
  } else {
    // Invalid input for month
    Browser.msgBox('Invalid input for month. Please enter a number between 1 and 12.');
  }
}

function isInteger(value) {
  return !isNaN(value) && parseInt(Number(value)) == value && !isNaN(parseInt(value, 10));
}
