DYNAMIC_PROPS = [
    "created_by",
    "updated_by",
    "created",
    "metadata_updated",
    "updated",
    "source_collection",
    "segments_updated",
]

ID_404 = "00000000-0000-1000-8000-00000000000a"

VIDEO_FLOW = {
    "id": "10000000-0000-1000-8000-000000000000",
    "source_id": "00000000-0000-1000-8000-000000000000",
    "format": "urn:x-nmos:format:video",
    "generation": 0,
    "label": "pytest - video",
    "description": "pytest - video",
    "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    "codec": "video/h264",
    "container": "video/mp2t",
    "avg_bit_rate": 5000000,
    "max_bit_rate": 5000000,
    "essence_parameters": {
        "frame_rate": {"numerator": 50, "denominator": 1},
        "frame_width": 1920,
        "frame_height": 1080,
        "bit_depth": 8,
        "interlace_mode": "progressive",
        "component_type": "YCbCr",
        "horiz_chroma_subs": 2,
        "vert_chroma_subs": 1,
        "avc_parameters": {"profile": 122, "level": 42, "flags": 0},
    },
}

IMAGE_FLOW = {
    "id": "10000000-0000-1000-8000-000000000004",
    "source_id": "00000000-0000-1000-8000-000000000004",
    "format": "urn:x-tam:format:image",
    "generation": 0,
    "label": "pytest - image",
    "description": "pytest - image",
    "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    "codec": "video/jpeg",
    "container": "video/jpeg",
    "essence_parameters": {
        "frame_width": 320,
        "frame_height": 180,
    },
}

AUDIO_FLOW = {
    "id": "10000000-0000-1000-8000-000000000001",
    "source_id": "00000000-0000-1000-8000-000000000001",
    "format": "urn:x-nmos:format:audio",
    "generation": 0,
    "label": "pytest - audio",
    "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    "codec": "audio/aac",
    "container": "video/mp2t",
    "essence_parameters": {
        "sample_rate": 48000,
        "channels": 2,
        "bit_depth": 32,
        "codec_parameters": {"coded_frame_size": 1024, "mp4_oti": 2},
    },
}

DATA_FLOW = {
    "id": "10000000-0000-1000-8000-000000000002",
    "source_id": "00000000-0000-1000-8000-000000000002",
    "format": "urn:x-nmos:format:data",
    "generation": 0,
    "label": "pytest - data",
    "description": "pytest - data",
    "tags": {"input_quality": "contribution", "flow_status": "ingesting"},
    "codec": "text/plain",
    "essence_parameters": {
        "data_type": "text",
    },
    "read_only": True,
}

MULTI_FLOW = {
    "id": "10000000-0000-1000-8000-000000000003",
    "source_id": "00000000-0000-1000-8000-000000000003",
    "format": "urn:x-nmos:format:multi",
    "generation": 0,
    "label": "pytest",
    "description": "pytest",
    "tags": {
        "input_quality": "contribution",
        "flow_status": "ingesting",
        "test": "this",
    },
    "container": "video/mp2t",
    "flow_collection": [
        {"id": "10000000-0000-1000-8000-000000000000", "role": "video"},
        {"id": "10000000-0000-1000-8000-000000000001", "role": "audio"},
        {"id": "10000000-0000-1000-8000-000000000002", "role": "data"},
        {"id": "10000000-0000-1000-8000-000000000004", "role": "image"},
    ],
}

TEST_FLOW = {
    "id": "10000000-0000-1000-8000-999999999999",
    "source_id": "00000000-0000-1000-8000-999999999999",
    "format": "urn:x-nmos:format:multi",
    "generation": 0,
    "label": "test flow",
    "description": "test flow",
    "tags": {
        "input_quality": "contribution",
        "flow_status": "ingesting",
        "test": "this",
    },
    "container": "video/mp2t",
}


def get_source(flow):
    return {
        "id": flow["source_id"],
        **{
            prop: flow[prop]
            for prop in ["format", "label", "description", "tags"]
            if prop in flow
        },
    }
