"""
This module is used to process azure buckets.
This includes:
    - List all the public objects
    - Process the xml response
"""

import requests
from xml.etree import ElementTree
from database_operation import *


def process_blob(blob):
    """
    Process the azure xml response
    :param blob:
    :return:
    """
    name = blob.find('Name').text
    url = blob.find('Url').text
    properties = blob.find('Properties')
    last_modified_str = properties.find('Last-Modified').text
    last_modified = datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')
    etag = properties.find('Etag').text
    content_length = properties.find('Content-Length').text
    content_type = properties.find('Content-Type').text
    content_md5 = properties.find('Content-MD5').text
    blob_type = properties.find('BlobType').text
    lease_status = properties.find('LeaseStatus').text

    return {
        'Name': name,
        'Url': url,
            'Last-Modified': last_modified,
        'ETag': etag,
        'Content-Length': content_length,
        'Content-Type': content_type,
        'Content-MD5': content_md5,
        'BlobType': blob_type,
        'LeaseStatus': lease_status,

    }

def extract_bucket_name_from_url(url):
    """
    Extract the bucket name from the url
    :param url:
    :return:
    """
    bucket=url.split("//")[1].split(".")[0]
    return bucket
def process_public_azure_bucket(azure_bucket_url,
                                next_marker=None,
                                azure_bucket_name=None,
                                max_results=1000):
    """
    This function will list the public objects in the bucket
    The objects will be pushed to the database for latter processing


    :param azure_bucket_url:
    :param next_marker:
    :param azure_bucket_name:
    :param max_results:
    :return:
    """
    start_time = datetime.now()
    page_count = 0
    checked_for_progress = False
    if not azure_bucket_name:
        azure_bucket_name = extract_bucket_name_from_url(azure_bucket_url)
    while True:
        # Define the request
        # Check if we can continue from where we left
        last_progress = get_last_item(
            db_name=azure_bucket_name,
            collection_name="progress"
        )

        if last_progress and not checked_for_progress:


            next_marker = last_progress[0]["NextMarker"]
            page_count= last_progress[0]["pageCount"]
            checked_for_progress = True
            print(f"Continuing from page {page_count} with marker {next_marker}")
            continue
        if next_marker:
            request = requests.get(azure_bucket_url, params={'marker': next_marker,
                                                       "restype":"container",
                                                       "comp":"list",
                                                       "maxresults":max_results})
        else:
            # First request
            request = requests.get(azure_bucket_url,
                                   params={
                                           "restype": "container",
                                           "comp": "list",
                                           "maxresults": 1000}
                                   )


        # Check if response is not OK so that we exit
        if request.status_code != 200:
            print(f"Error: {request.status_code}")
            break
        # Response is xml, parse the exml
        root = ElementTree.fromstring(request.content)

        # Extract and process all blobs
        blobs = []
        for blob in root.findall('.//Blob'):
            blob_data = process_blob(blob)
            blobs.append(blob_data)

        # Send the data to the database
        insert_many(db_name=azure_bucket_name,
                    collection_name="fileList",
                    data=blobs)

        # Extract the NextMarker value
        next_marker = root.find('NextMarker')

        if next_marker is not None:
            next_marker = next_marker.text
            insert_one(db_name=azure_bucket_name,
                       collection_name="progress",
                       data={"NextMarker": next_marker,
                             "pageCount":page_count})
        else:
            break




        # Print the number of blobs
        print(f"Page {page_count} - {len(blobs)} blobs")
        page_count += 1

    end_time = datetime.now()
    total_time = end_time - start_time
    print(f"Total time: {total_time}")
    print(f"Total Pages: {page_count}")
if __name__ == "__main__":
    bucket_name = "littleimages"
    bucket_url = f"https://littleimages.blob.core.windows.net/documents/"
    # process_public_azure_bucket(bucket_url, azure_bucket_name=bucket_name)
    name=extract_bucket_name_from_url(bucket_url)
    print(name)