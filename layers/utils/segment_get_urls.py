import concurrent.futures
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from aws_lambda_powertools import Tracer
from dynamodb import get_default_storage_backend, get_storage_backend, get_store_name
from utils import generate_presigned_url

tracer = Tracer()


@tracer.capture_method(capture_response=False)
def create_presigned_get_url(s3_url: str) -> Tuple[str, str]:
    """Generate a presigned URL for S3 object access.

    Args:
        s3_url: The original S3 URL

    Returns:
        Tuple of (original_url, presigned_url)
    """
    url_parse = urlparse(s3_url)
    url = generate_presigned_url(
        "get_object", url_parse.netloc.split(".")[0], url_parse.path.split("/", 1)[1]
    )
    return (s3_url, url)


@tracer.capture_method(capture_response=False)
def create_direct_s3_get_url(
    object_id: str, storage_backend: Dict[str, str]
) -> Dict[str, str]:
    """Generate a non-signed S3 URL for object access.

    Args:
        object_id: The S3 object identifier
        storage_backend: Storage backend configuration dict

    Returns:
        Dict containing label and URL
    """
    store_name = get_store_name()
    return {
        "label": f'aws.{storage_backend["region"]}:s3:{store_name}',
        "url": f'https://{storage_backend["bucket_name"]}.s3.{storage_backend["region"]}.amazonaws.com/{object_id}',
    }


@tracer.capture_method(capture_response=False)
def create_presigned_urls_parallel(url_set: Set[str]) -> Dict[str, str]:
    """Generate presigned URLs in parallel using ThreadPoolExecutor.

    Args:
        urls: Set of s3 URLs to generate presigned URLs for

    Returns:
        Dict mapping original URLs to presigned URLs
    """
    # Asynchronous call to pre-signed url API
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for s3_url in url_set:
            futures.append(executor.submit(create_presigned_get_url, s3_url))
    # Build dict of returned urls
    url_mapping = {}
    for future in futures:
        key, url = future.result()
        url_mapping[key] = url
    return url_mapping


@tracer.capture_method(capture_response=False)
def get_storage_backends(
    accept_storage_ids: Optional[str], segments: List[Dict]
) -> Dict[str, Dict]:
    """Get storage backend configurations for segments.

    Args:
        accept_storage_ids: Comma-separated storage IDs to filter by
        segments: List of segment dictionaries

    Returns:
        Dict mapping storage IDs to their backend configurations
    """
    filter_ids = accept_storage_ids.split(",") if accept_storage_ids else None
    distinct_storage_ids = set(
        storage_id
        for segment in segments
        for storage_id in segment.get("storage_ids", [])
    )
    filtered_storage_ids = (
        [storage_id for storage_id in distinct_storage_ids if storage_id in filter_ids]
        if filter_ids
        else distinct_storage_ids
    )
    return {
        storage_id: get_storage_backend(storage_id)
        for storage_id in filtered_storage_ids
    }


@tracer.capture_method(capture_response=False)
def create_segment_access_urls(
    segment: Dict,
    storage_backend: Dict,
    generate_presigned_urls: bool,
    verbose_storage: bool,
) -> List[Dict]:
    """Generate controlled access URLs for a segment.

    Args:
        segment: Segment dictionary containing object_id
        storage_backend: Storage backend configuration
        generate_presigned_urls: Whether to generate presigned URLs
        verbose_storage: Whether to include verbose storage info

    Returns:
        List of URL dictionaries with labels and URLs
    """
    direct_url = create_direct_s3_get_url(segment["object_id"], storage_backend)
    if verbose_storage:
        direct_url = {
            **direct_url,
            **storage_backend,
            "controlled": True,
        }
    get_urls = [direct_url]
    if generate_presigned_urls:
        presigned_url_template = direct_url.copy()
        presigned_url_template["label"] = presigned_url_template["label"].replace(
            ":s3:", ":s3.presigned:"
        )
        presigned_url_template["presigned"] = True
        get_urls.append(presigned_url_template)
    return get_urls


@tracer.capture_method(capture_response=False)
def populate_get_urls(
    segments: List[Dict],
    accept_get_urls: Optional[str],
    verbose_storage: Optional[bool],
    accept_storage_ids: Optional[str],
    presigned: Optional[bool],
) -> None:
    """Populate the object get_urls based on the supplied parameters.

    Args:
        segments: List of segment dictionaries to populate URLs for
        accept_get_urls: Comma-separated list of URL labels to accept
        verbose_storage: Whether to include verbose storage information
        accept_storage_ids: Comma-separated storage IDs to filter by
        presigned: Whether to generate presigned URLs only
    """
    # Early return if no urls are requested
    if accept_get_urls == "":
        for segment in segments:
            segment["get_urls"] = []
        return
    should_create_presigned_urls = (presigned or presigned is None) and (
        accept_get_urls is None or ":s3.presigned:" in accept_get_urls
    )
    filter_labels = (
        None if accept_get_urls is None else accept_get_urls.split(",")
    )  # Test explictly for None as empty string has special meaning but would be falsey
    storage_backends = get_storage_backends(accept_storage_ids, segments)
    default_storage_backend = get_default_storage_backend()
    default_storage_available = (
        storage_backends.get(default_storage_backend["id"]) is not None
    )
    # Keep track of unique needed presigned urls during the loop
    urls_needing_presigning = set()
    for segment in segments:
        storage_ids = segment.get("storage_ids", [])
        get_urls = [] if accept_storage_ids else segment.get("get_urls", [])
        if not storage_ids and not segment.get("get_urls"):
            if default_storage_available:
                get_urls.extend(
                    create_segment_access_urls(
                        segment,
                        default_storage_backend,
                        should_create_presigned_urls,
                        verbose_storage,
                    )
                )
        for storage_id in storage_ids:
            if storage_backends.get(storage_id):
                get_urls.extend(
                    create_segment_access_urls(
                        segment,
                        storage_backends[storage_id],
                        should_create_presigned_urls,
                        verbose_storage,
                    )
                )
        if filter_labels:
            get_urls = [
                get_url for get_url in get_urls if get_url["label"] in filter_labels
            ]
        if presigned is not None:
            get_urls = [
                get_url
                for get_url in get_urls
                if get_url.get("presigned", False) == presigned
            ]
        # Add needed presigned urls to set for later parallel processing
        urls_needing_presigning.update(
            get_url["url"] for get_url in get_urls if get_url.get("presigned", False)
        )
        segment["get_urls"] = get_urls
    # Generate presigned urls for all unique urls needed
    presigned_url_mapping = create_presigned_urls_parallel(urls_needing_presigning)
    # Update the url for the presigned get_urls present
    for segment in segments:
        get_urls = []
        for get_url in segment["get_urls"]:
            if get_url.get("presigned", False):
                get_urls.append(
                    {**get_url, "url": presigned_url_mapping[get_url["url"]]}
                )
            else:
                get_urls.append(get_url)
        segment["get_urls"] = get_urls
