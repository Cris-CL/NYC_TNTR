from __future__ import print_function

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def fetch_changes(saved_start_page_token):
    """Retrieve the list of changes for the currently authenticated user.
        prints changed file's ID
    Args:
        saved_start_page_token : StartPageToken for the current state of the
        account.
    Returns: saved start page token.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    creds, _ = google.auth.default()
    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        # Begin with our last saved start token for this user or the
        # current token from getStartPageToken()
        page_token = saved_start_page_token
        # pylint: disable=maybe-no-member

        while page_token is not None:
            response = service.changes().list(pageToken=page_token,
                                              spaces='drive').execute()
            for change in response.get('changes'):
                # Process change
                print(F'Change found for file: {change.get("fileId")}')
            if 'newStartPageToken' in response:
                # Last page, save this token for the next polling interval
                saved_start_page_token = response.get('newStartPageToken')
            page_token = response.get('nextPageToken')

    except HttpError as error:
        print(F'An error occurred: {error}')
        saved_start_page_token = None

    return saved_start_page_token


if __name__ == '__main__':
    # saved_start_page_token is the token number
    fetch_changes(saved_start_page_token=209)
