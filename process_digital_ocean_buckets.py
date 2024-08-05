from datetime import datetime

import requests
from xml.etree import ElementTree
from database_operation import *

"""
This module will contain functions for processing all digital ocean buckets.
These operations include:
    - List public objects
    - Extract the value keys from the xml
    - Insert the values into the database
    - Determine the file path from the name of the file.
"""

def list_public_objects(bucket_url, next_marker=None):
    """
    This function will list the public objects in the bucket
    The objects will be pushed to the database for latter processing
    This module currently only support Mongo DB

    :param bucket_url:
    :param next_marker:
    :return:
    """
    page_count = 0
    checked_for_progress = False
    while True:
        # Define the request
        if next_marker:
            request = requests.get(bucket_url, params={'marker': next_marker})
        else:
            # First request
            request = requests.get(bucket_url)
        # Response is xml, parse the exml
        root = ElementTree.fromstring(request.content)

        # Define the namespace
        namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

        # Extract information
        bucket_name = root.find('s3:Name', namespace).text
        max_keys = root.find('s3:MaxKeys', namespace).text
        is_truncated = root.find('s3:IsTruncated', namespace).text




        # Check if we can continue from where we left
        last_progress= get_last_item(
            bucket_name,"progress"
        )

        if last_progress and not checked_for_progress:
            next_marker = last_progress[0]['next_marker']
            page_count = last_progress[0]['page_count']
            print(f"Continuing from page {page_count} with marker {next_marker}")
            checked_for_progress = True
            continue
        else:
            print(f"Starting from page {page_count} with marker {next_marker}")

        contents = []
        for content in root.findall('s3:Contents', namespace):
            key = content.find('s3:Key', namespace).text
            last_modified = content.find('s3:LastModified', namespace).text
            etag = content.find('s3:ETag', namespace).text
            size = content.find('s3:Size', namespace).text
            storage_class = content.find('s3:StorageClass', namespace).text
            owner_id = content.find('s3:Owner/s3:ID', namespace).text
            owner_display_name = content.find('s3:Owner/s3:DisplayName', namespace).text

            contents.append({
                'Key': key,
                'LastModified': last_modified,
                'ETag': etag,
                'Size': size,
                'StorageClass': storage_class,
                'OwnerID': owner_id,
                'OwnerDisplayName': owner_display_name,
                "fileType": generate_extension_from_name(key)
            })


        insert_many(bucket_name, "fileList", contents)

        if is_truncated == 'false':
            break
        else:
            marker = root.find('s3:NextMarker', namespace).text
            next_marker = marker
        page_count += 1
        print(f"Page {page_count} done")

        insert_one(bucket_name, "progress", {"page_count": page_count,
                                              "next_marker": next_marker})


def generate_extension_from_name(name):
    """
    This function will generate the file extension from the name of the file

    TODO: Try to get the file type from the metadata.
    :param name:
    :return:
    """
    split_name = name.split(".")
    if len(split_name) > 1:
        return split_name[-1]
    return "Unknown"



if __name__ == '__main__':

    """
    Test the bucket listing process with a public bucket
    """

    do_bucket_url = "https://mookhfiles.fra1.digitaloceanspaces.com/"

    list_public_objects(do_bucket_url)
