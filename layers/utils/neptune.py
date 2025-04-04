import json
import os
from datetime import datetime, timezone
from time import sleep

import boto3

# pylint: disable=no-member
import constants
import cymple
from aws_lambda_powertools import Tracer
from cymple import QueryBuilder
from deepdiff import DeepDiff
from schema import Flowcollection, Source
from utils import (
    deserialise_neptune_obj,
    filter_dict,
    model_dump,
    parse_parameters,
    publish_event,
    serialise_neptune_obj,
)

tracer = Tracer()

neptune = boto3.client(
    "neptunedata",
    region_name=os.environ["AWS_REGION"],
    endpoint_url=f'https://{os.environ["NEPTUNE_ENDPOINT"]}:8182',
)
qb = QueryBuilder()


@tracer.capture_method(capture_response=False)
def execute_open_cypher_query(query, max_retries=3, base_delay=0.01):
    for attempt in range(max_retries + 1):
        try:
            return neptune.execute_open_cypher_query(openCypherQuery=query)
        except neptune.exceptions.BadRequestException as e:
            if attempt == max_retries:
                print(f"Failed after {max_retries} retries: {e}")
                raise e
            delay = base_delay * (2**attempt)
            print(f"Attempt {attempt + 1} failed, retrying in {delay} seconds: {e}")
            sleep(delay)


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def generate_source_query(
    properties: dict, set_dict: dict = None, where_literals: list = []
) -> cymple.builder.NodeAvailable:
    """Returns an Open Cypher Match query to return specified Source"""
    query = (
        qb.match()
        .node(ref_name="f", labels="flow", properties=properties.get("flow", {}))
        .related_to(label="represents")
        .node(
            ref_name="source",
            labels="source",
            properties=properties.get("source", {}),
        )
        .related_to(label="has_tags")
        .node(ref_name="t", labels="tags", properties=properties.get("tags", {}))
    )
    if set_dict:
        query = query.set(set_dict).with_("*")
    if len(where_literals) > 0:
        query = query.where_literal(" AND ".join(where_literals))
    query = (
        query.match_optional()
        .node(ref_name="f")
        .related_from(ref_name="c", label="collected_by")
        .node(labels="flow")
        .related_to(label="represents")
        .node(ref_name="sc", labels="source")
        .match_optional()
        .node(ref_name="f")
        .related_to(label="collected_by")
        .node(labels="flow")
        .related_to(label="represents")
        .node(ref_name="cb", labels="source")
    )
    return query


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def generate_flow_query(
    properties: dict, set_dict: dict = None, where_literals: list = []
) -> cymple.builder.NodeAvailable:
    """Returns an Open Cypher Match query to return specified Flow"""
    query = (
        qb.match()
        .node(
            ref_name="flow",
            labels="flow",
            properties=properties.get("flow", {}),
        )
        .related_to(label="represents")
        .node(
            ref_name="s",
            labels="source",
            properties=properties.get("source", {}),
        )
        .match()
        .node(ref_name="flow")
        .related_to(label="has_essence_parameters")
        .node(
            ref_name="e",
            labels="essence_parameters",
            properties=properties.get("essence_parameters", {}),
        )
        .match()
        .node(ref_name="flow")
        .related_to(label="has_tags")
        .node(ref_name="t", labels="tags", properties=properties.get("tags", {}))
    )
    if set_dict:
        query = query.set(set_dict).with_("*")
    if len(where_literals) > 0:
        query = query.where_literal(" AND ".join(where_literals))
    query = (
        query.match_optional()
        .node(ref_name="flow")
        .related_from(ref_name="c", label="collected_by")
        .node(ref_name="fc", labels="flow")
        .match_optional()
        .node(ref_name="flow")
        .related_to(label="collected_by")
        .node(ref_name="cb", labels="flow")
    )
    return query


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def generate_delete_request_query(
    properties: dict, set_dict: dict = None, where_literals: list = []
) -> cymple.builder.NodeAvailable:
    """Returns an Open Cypher Match query to return specified Delete Request"""
    query = (
        qb.match()
        .node(
            ref_name="delete_request",
            labels="delete_request",
            properties=properties.get("delete_request", {}),
        )
        .related_to(label="has_error")
        .node(ref_name="e", labels="error", properties=properties.get("error", {}))
    )
    if set_dict:
        query = query.set(set_dict).with_("*")
    if len(where_literals) > 0:
        query = query.where_literal(" AND ".join(where_literals))
    return query


@tracer.capture_method(capture_response=False)
# pylint: disable=dangerous-default-value
def generate_match_query(
    record_type: str, properties: dict, set_dict: dict = None, where_literals: list = []
) -> cymple.builder.NodeAvailable:
    """Returns an Open Cypher Match query to return specified record type"""
    match record_type:
        case "source":
            return generate_source_query(properties, set_dict, where_literals)
        case "flow":
            return generate_flow_query(properties, set_dict, where_literals)
        case "delete_request":
            return generate_delete_request_query(properties, set_dict, where_literals)


@tracer.capture_method(capture_response=False)
def check_delete_source(source_id: str) -> bool:
    """Performs a conditional delete on the specified Source. It is only deleted if it is not referenced by any flow representations. Returns True if delete occurred."""
    query = (
        qb.match()
        .node(ref_name="source", labels="source", properties={"id": source_id})
        .where_literal("NOT exists((source)<-[:represents]-(:flow))")
        .match()
        .node(ref_name="source")
        .related_to(label="has_tags")
        .node(ref_name="t", labels="tags")
        .detach_delete(ref_name="source")
        .return_literal("source.id AS source_id")
        .get()
    )
    query = query.replace(
        "DETACH DELETE source", "DETACH DELETE source DELETE t"
    )  # Limitation in Cymple library, unable to stack DELETE
    results = execute_open_cypher_query(query)
    return len(results["results"]) > 0


@tracer.capture_method(capture_response=False)
def get_flow_source_id(flow_id: str) -> str | None:
    """Get the source_id for the specified Flow"""
    try:
        query = (
            qb.match()
            .node(labels="flow", properties={"id": flow_id})
            .related_to(label="represents")
            .node(ref_name="s", labels="source")
            .return_literal("s.id as source_id")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["source_id"]
    except IndexError:
        return None


@tracer.capture_method(capture_response=False)
def get_source_collected_by(source_id: str) -> list:
    """Get the collect_by source ids for the specified Source"""
    try:
        query = (
            qb.match()
            .node(labels="source", properties={"id": source_id})
            .related_from(label="represents")
            .node(labels="flow")
            .related_to(label="collected_by")
            .node(labels="flow")
            .related_to(label="represents")
            .node(ref_name="s", labels="source")
            .return_literal("collect(s.id) as source_collected_by")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["source_collected_by"]
    except IndexError:
        return []


@tracer.capture_method(capture_response=False)
def get_flow_collected_by(flow_id: str) -> list:
    """Get the collect_by flow ids for the specified Flow"""
    try:
        query = (
            qb.match()
            .node(labels="flow", properties={"id": flow_id})
            .related_to(label="collected_by")
            .node(ref_name="f", labels="flow")
            .return_literal("collect(f.id) as flow_collected_by")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["flow_collected_by"]
    except IndexError:
        return []


@tracer.capture_method(capture_response=False)
def query_node(record_type: str, record_id: str) -> dict:
    """Returns the specified Node from the Neptune Database"""
    try:
        query = (
            generate_match_query(record_type, {record_type: {"id": record_id}})
            .return_literal(constants.RETURN_LITERAL[record_type])
            .get()
        )
        results = execute_open_cypher_query(query)
        deserialised_results = [
            deserialise_neptune_obj(result[record_type])
            for result in results["results"]
        ]
        return deserialised_results[0]
    except IndexError as e:
        raise ValueError("No results returned from the database query.") from e


@tracer.capture_method(capture_response=False)
def check_node_exists(record_type: str, record_id: str) -> bool:
    """Checks whether the specified Node exists in the Neptune Database"""
    query = (
        qb.match()
        .node(ref_name="n", labels=record_type, properties={"id": record_id})
        .return_literal("n.id")
        .get()
    )
    results = execute_open_cypher_query(query)
    return len(results["results"]) > 0


@tracer.capture_method(capture_response=False)
def query_node_tags(record_type: str, record_id: str) -> dict:
    """Returns the TAMS Tags for the specified Node"""
    try:
        query = (
            qb.match()
            .node(labels=record_type, properties={"id": record_id})
            .related_to(label="has_tags")
            .node(ref_name="t", labels="tags")
            .return_literal("t {.*} AS tags")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["tags"]
    except IndexError as e:
        raise ValueError("No results returned from the database query.") from e


@tracer.capture_method(capture_response=False)
def query_node_property(record_type: str, record_id: str, prop_name: str) -> any:
    """Returns the value of the specified Node property"""
    try:
        query = (
            qb.match()
            .node(ref_name="n", labels=record_type, properties={"id": record_id})
            .return_literal(f"n.{prop_name} AS property")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["property"]
    except IndexError as e:
        raise ValueError("No results returned from the database query.") from e


@tracer.capture_method(capture_response=False)
def query_flow_collection(flow_id: str) -> list:
    """Returns the flow_collection of the specified Flow"""
    try:
        query = (
            qb.match()
            .node(ref_name="f", labels="flow", properties={"id": flow_id})
            .match_optional()
            .node(ref_name="f")
            .related_from(ref_name="c", label="collected_by")
            .node(ref_name="fc", labels="flow")
            .return_literal("f.id as id, collect(c {.*, id: fc.id}) AS flow_collection")
            .get()
        )
        results = execute_open_cypher_query(query)
        return results["results"][0]["flow_collection"]
    except IndexError as e:
        raise ValueError("No results returned from the database query.") from e


@tracer.capture_method(capture_response=False)
def query_sources(parameters: dict) -> tuple[list, int]:
    """Returns a list of the TAMS Sources from the Neptune Database"""
    props, where_literals = parse_parameters(
        {
            k: v
            for k, v in parameters.items()
            if k
            in [
                "format",
                "label",
                "tag_values",
                "tag_exists",
            ]
        }
    )
    page = int(parameters["page"]) if parameters.get("page") else 0
    limit = min(
        (
            parameters["limit"]
            if parameters.get("limit")
            else constants.DEFAULT_PAGE_LIMIT
        ),
        constants.MAX_PAGE_LIMIT,
    )
    query = generate_source_query(
        {
            "source": props["properties"],
            "tags": props["tag_properties"],
        },
        where_literals=where_literals,
    )
    query = (
        query.return_literal(constants.RETURN_LITERAL["source"])
        .order_by("source.id")
        .skip(page)
        .limit(limit)
        .get()
    )
    results = execute_open_cypher_query(query)
    deserialised_results = [
        deserialise_neptune_obj(result["source"]) for result in results["results"]
    ]
    next_page = page + limit if len(deserialised_results) == limit else None
    return deserialised_results, next_page


@tracer.capture_method(capture_response=False)
def query_flows(parameters: dict) -> tuple[list, int]:
    """Returns a list of the TAMS Flows from the Neptune Database"""
    props, where_literals = parse_parameters(
        {
            k: v
            for k, v in parameters.items()
            if k
            in [
                "format",
                "codec",
                "label",
                "tag_values",
                "tag_exists",
                "frame_width",
                "frame_height",
            ]
        }
    )
    if parameters.get("source_id"):
        props["source_properties"]["id"] = parameters["source_id"]
    page = int(parameters["page"]) if parameters.get("page") else 0
    limit = min(
        (
            parameters["limit"]
            if parameters.get("limit")
            else constants.DEFAULT_PAGE_LIMIT
        ),
        constants.MAX_PAGE_LIMIT,
    )
    query = generate_flow_query(
        {
            "flow": props["properties"],
            "source": props["source_properties"],
            "essence_parameters": props["essence_properties"],
            "tags": props["tag_properties"],
        },
        where_literals=where_literals,
    )
    query = (
        query.return_literal(constants.RETURN_LITERAL["flow"])
        .order_by("flow.id")
        .skip(page)
        .limit(limit)
        .get()
    )
    results = execute_open_cypher_query(query)
    deserialised_results = [
        deserialise_neptune_obj(result["flow"]) for result in results["results"]
    ]
    next_page = page + limit if len(deserialised_results) == limit else None
    return deserialised_results, next_page


@tracer.capture_method(capture_response=False)
def query_delete_requests() -> list:
    """Returns a list of the TAMS Delete Request from the Neptune Database"""
    query = generate_delete_request_query({})
    query = (
        query.return_literal(constants.RETURN_LITERAL["delete_request"])
        .order_by("delete_request.id")
        .get()
    )
    results = execute_open_cypher_query(query)
    deserialised_results = [
        deserialise_neptune_obj(result["delete_request"])
        for result in results["results"]
    ]
    return deserialised_results


@tracer.capture_method(capture_response=False)
def merge_source(source_dict: dict) -> None:
    """Perform an OpenCypher Merge operation on the supplied TAMS Source record"""
    tags = source_dict.get("tags", {})
    query = (
        qb.merge()
        .node(
            ref_name="s",
            labels="source",
            properties={"id": source_dict["id"]},
        )
        .related_to(label="has_tags")
        .node(ref_name="t", labels="tags")
        .set(serialise_neptune_obj(filter_dict(source_dict, {"id", "tags"}), "s."))
    )
    # Add Set for Source Tags
    if tags:
        query = query.set(serialise_neptune_obj(tags, "t."))
    execute_open_cypher_query(query.get())


@tracer.capture_method(capture_response=False)
def merge_flow(flow_dict: dict, existing_dict: dict) -> dict:
    """Perform an OpenCypher Merge operation on the supplied TAMS Flow record"""
    # Extract properties required for other node types
    tags = flow_dict.get("tags", {})
    essence_parameters = flow_dict.get("essence_parameters", {})
    flow_collection = flow_dict.get("flow_collection", [])
    flow_properties = filter_dict(
        flow_dict,
        {"id", "source_id", "tags", "essence_parameters", "flow_collection"},
    )
    if existing_dict:
        existing_flow_collection = existing_dict.get("flow_collection", [])
        # If there is an flow collection and it has changed delete it so that it is set correctly with the merge
        if existing_flow_collection and DeepDiff(
            flow_collection, existing_flow_collection, ignore_order=True
        ):
            set_flow_collection(flow_dict["id"], "temp", [])
        null_tags = {
            k: None for k in existing_dict.get("tags", {}).keys() - tags.keys()
        }
        null_essence_parameters = {
            k: None
            for k in existing_dict.get("essence_parameters", {}).keys()
            - essence_parameters.keys()
        }
        null_properties = {
            k: None
            for k in (
                existing_dict.keys()
                - {
                    "id",
                    "source_id",
                    "tags",
                    "essence_parameters",
                    "flow_collection",
                    "created_by",
                    "updated_by",
                }
            )
            - flow_properties.keys()
        }
        tags = tags | null_tags
        essence_parameters = essence_parameters | null_essence_parameters
        flow_properties = flow_properties | null_properties
    # Build Merge queries
    query = (
        qb.match()
        .node(ref_name="s", labels="source", properties={"id": flow_dict["source_id"]})
        .merge()
        .node(ref_name="f", labels="flow", properties={"id": flow_dict["id"]})
        .merge()
        .node(ref_name="f")
        .related_to(label="represents")
        .node(ref_name="s")
        .merge()
        .node(ref_name="f")
        .related_to(label="has_tags")
        .node(ref_name="t", labels="tags")
        .merge()
        .node(ref_name="f")
        .related_to(label="has_essence_parameters")
        .node(ref_name="ep", labels="essence_parameters")
        .set(serialise_neptune_obj(flow_properties, "f."))
    )
    # Add Set for Flow Tags
    if tags:
        query = query.set(serialise_neptune_obj(tags, "t."))
    # Add Set for Flow Essence Parameters
    if essence_parameters:
        query = query.set(serialise_neptune_obj(essence_parameters, "ep."))
    # Add flow collection queries as needed
    query_collection = generate_flow_collection_query(flow_collection)
    if query_collection:
        query = query + query_collection
    execute_open_cypher_query(query.get())
    # Check if source was updated
    if (
        existing_dict.get("source_id")
        and flow_dict["source_id"] != existing_dict["source_id"]
    ):
        # Delete the old represents edge
        query_delete = (
            qb.match()
            .node(labels="flow", properties={"id": existing_dict["id"]})
            .related_to(ref_name="r", label="represents")
            .node(labels="source", properties={"id": existing_dict["source_id"]})
            .delete(ref_name="r")
            .get()
        )
        execute_open_cypher_query(query_delete)
        # Delete source if no longer referenced by any other flows
        if check_delete_source(existing_dict["source_id"]):
            publish_event(
                "sources/deleted",
                {"source_id": existing_dict["source_id"]},
                enhance_resources([f'tams:source:{existing_dict["source_id"]}']),
            )
    # Too complex to try and get OpenCypher to return the object in the same query so calling the DB to get it separately
    return query_node("flow", flow_dict["id"])


@tracer.capture_method(capture_response=False)
def merge_delete_request(delete_request_dict: dict) -> None:
    """Perform an OpenCypher Merge operation on the supplied TAMS Delete Request record"""
    error = delete_request_dict.get("error", {})
    query = (
        qb.merge()
        .node(
            ref_name="d",
            labels="delete_request",
            properties={"id": delete_request_dict["id"]},
        )
        .related_to(label="has_error")
        .node(ref_name="e", labels="error")
        .set(
            serialise_neptune_obj(
                filter_dict(delete_request_dict, {"id", "error"}), "d."
            )
        )
    )
    # Add Set for Error
    if error:
        query = query.set(serialise_neptune_obj(error, "e."))
    execute_open_cypher_query(query.get())


@tracer.capture_method(capture_response=False)
def delete_flow(flow_id: str) -> str | None:
    """Deletes the specified Flow from the Neptune Database"""
    try:
        query = (
            qb.match()
            .node(ref_name="flow", labels="flow", properties={"id": flow_id})
            .related_to(label="represents")
            .node(ref_name="s", labels="source")
            .match()
            .node(ref_name="flow")
            .related_to(label="has_tags")
            .node(ref_name="t", labels="tags")
            .match()
            .node(ref_name="flow")
            .related_to(label="has_essence_parameters")
            .node(ref_name="e", labels="essence_parameters")
            .detach_delete(ref_name="flow")
            .return_literal("s.id AS source_id")
            .get()
        )
        query = query.replace(
            "DETACH DELETE flow", "DETACH DELETE flow DELETE t DELETE e"
        )  # Limitation in Cymple library, unable to stack DELETE
        results = execute_open_cypher_query(query)
        return results["results"][0]["source_id"]
    except IndexError:
        return None


@tracer.capture_method(capture_response=False)
def set_node_property_base(record_type: str, record_id: str, props: dict) -> dict:
    """Performs an OpenCypher Set operation on the specified Node and properties"""
    try:
        query = (
            generate_match_query(
                record_type,
                {record_type: {"id": record_id}},
                set_dict=props,
            )
            .return_literal(constants.RETURN_LITERAL[record_type])
            .get()
        )
        results = execute_open_cypher_query(query)
        deserialised_results = [
            deserialise_neptune_obj(result[record_type])
            for result in results["results"]
        ]
        return deserialised_results[0]
    except IndexError as e:
        raise ValueError("No results returned from the database query.") from e


@tracer.capture_method(capture_response=False)
def set_node_property(
    record_type: str, record_id: str, username: str, props: dict
) -> dict:
    """Performs an OpenCypher Set operation on the specified Node and properties with the addition of updated and updated_by properties"""
    meta_props = {
        **props,
        f"{record_type}.{"metadata_" if record_type == "flow" else ""}updated": datetime.now()
        .astimezone(timezone.utc)
        .strftime(constants.DATETIME_FORMAT),
        f"{record_type}.updated_by": username,
    }
    return set_node_property_base(record_type, record_id, meta_props)


@tracer.capture_method(capture_response=False)
def set_flow_collection(flow_id: str, username: str, flow_collection: list) -> dict:
    """Set the specfified flow to have the specified flow_collection and set flow metadata_updated and updated_by properties"""
    # Create initial query that deletes all collect_by edges for this flow
    query = (
        qb.match()
        .node(ref_name="f", labels="flow", properties={"id": flow_id})
        .match_optional()
        .node(ref_name="f")
        .related_from(ref_name="c", label="collected_by")
        .node(labels="flow")
        .delete(ref_name="c")
    )
    # Create the base set query
    query_collection = generate_flow_collection_query(
        flow_collection,
        {
            "f.metadata_updated": datetime.now()
            .astimezone(timezone.utc)
            .strftime(constants.DATETIME_FORMAT),
            "f.updated_by": username,
        },
    )
    if query_collection:
        query = query + query_collection
    execute_open_cypher_query(query.get())
    # Too complex to try and get OpenCypher to return the object in the same query so calling the DB to get it separately
    return query_node("flow", flow_id)


@tracer.capture_method(capture_response=False)
def update_flow_segments_updated(flow_id: str) -> None:
    """Update the segments_updated field on the specified Flow"""
    try:
        item_dict = set_node_property_base(
            "flow",
            flow_id,
            {
                "flow.segments_updated": datetime.now()
                .astimezone(timezone.utc)
                .strftime(constants.DATETIME_FORMAT)
            },
        )
        publish_event(
            "flows/updated",
            {"flow": item_dict},
            enhance_resources(
                [
                    f'tams:flow:{item_dict["id"]}',
                    f'tams:source:{item_dict["source_id"]}',
                    *set(
                        f"tams:flow-collected-by:{c_id}"
                        for c_id in item_dict.get("collected_by", [])
                    ),
                ]
            ),
        )
    except ValueError:
        # The set_node_property_base function will throw an exception
        # if specified flow does not exist. When setting the segments_updated
        # field in the database don't need to worry if the flow does not exist.
        return


@tracer.capture_method(capture_response=False)
def enhance_resources(resources) -> list:
    """Publishes the supplied events to an EventBridge EventBus"""
    if all(not r.startswith("tams:source:") for r in resources) and any(
        r.startswith("tams:flow:") for r in resources
    ):
        flow_id = next(
            r[len("tams:flow:") :] for r in resources if r.startswith("tams:flow:")
        )
        source_id = get_flow_source_id(flow_id)
        if source_id:
            resources.append(f"tams:source:{source_id}")
    if all(not r.startswith("tams:source-collected-by:") for r in resources) and any(
        r.startswith("tams:source:") for r in resources
    ):
        source_id = next(
            r[len("tams:source:") :] for r in resources if r.startswith("tams:source:")
        )
        resources.extend(
            set(
                f"tams:source-collected-by:{s_id}"
                for s_id in get_source_collected_by(source_id)
            )
        )
    if all(not r.startswith("tams:flow-collected-by:") for r in resources) and any(
        r.startswith("tams:flow:") for r in resources
    ):
        flow_id = next(
            r[len("tams:flow:") :] for r in resources if r.startswith("tams:flow:")
        )
        resources.extend(
            set(
                f"tams:flow-collected-by:{s_id}"
                for s_id in get_flow_collected_by(flow_id)
            )
        )
    return resources


@tracer.capture_method(capture_response=False)
def validate_flow_collection(flow_id: str, flow_collection: Flowcollection):
    """Checks whether the supplied Flow Collection is valid"""
    if not flow_collection:
        return True
    for collection in flow_collection.root:
        if flow_id == collection.id or not check_node_exists("flow", collection.id):
            return False
    return True


@tracer.capture_method(capture_response=False)
def merge_source_flow(flow_dict: dict, existing_dict: dict) -> dict:
    """Perform an OpenCypher Merge operation on the supplied TAMS Source/Flow record"""
    # Check if supplied source already exists, create if not
    if not check_node_exists("source", flow_dict["source_id"]):
        source: Source = Source(**flow_dict)
        source.id = flow_dict["source_id"]
        source_dict = model_dump(source)
        merge_source(source_dict)
        publish_event(
            "sources/created",
            {"source": source_dict},
            enhance_resources([f'tams:source:{source_dict["id"]}']),
        )
    # Create Flow
    return merge_flow(flow_dict, existing_dict)


@tracer.capture_method(capture_response=False)
def generate_flow_collection_query(
    flow_collection: list, set_dict: dict | None = None
) -> cymple.builder.SetAvailable | None:
    """Returns a QueryBuilder that creates the specfied flow collection."""
    if not set_dict:
        set_dict = {}
    if not flow_collection:
        return None
    query = qb.with_("f")
    # process the supplied flow_collection into numbered records
    ref_names = []
    for n, collection in enumerate(flow_collection):
        collection_properties = {
            k: json.dumps(v) if isinstance(v, list) or isinstance(v, dict) else v
            for k, v in collection.items()
            if k != "id"
        }
        ref_names.append((f"f{n}", f"c{n}", collection["id"], collection_properties))
    # Add the match queries for each flow_collection record
    for f_ref, _, f_id, _ in ref_names:
        query = query.match().node(
            ref_name=f_ref, labels="flow", properties={"id": f_id}
        )
    # Add the merge queries to create the collected_by edges for each flow_collection record
    for f_ref, c_ref, _, _ in ref_names:
        query = (
            query.merge()
            .node(ref_name="f")
            .related_from(ref_name=c_ref, label="collected_by")
            .node(ref_name=f_ref)
        )
    # Build the dict of set operations to carry out
    for _, c_ref, _, props in ref_names:
        for k, v in props.items():
            set_dict[f"{c_ref}.{k}"] = v
    return query.set(set_dict)
