"""C1 contract fixtures: static A1 answers, never calculator outputs."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID

import pytest
from pydantic import ValidationError

from app.correlation.blast_radius import (
    MAX_CONTEXT_BYTES,
    MAX_HANDOFF_BYTES,
    BlastRadiusInputError,
    BlastRadiusLimitError,
    BlastRadiusPolicy,
    _canonical_bytes,
    _decode_object,
    compute_context_fingerprint,
    decode_blast_radius_context_structural,
    serialize_blast_radius_context,
)
from app.correlation.topology import TopologySnapshot, serialize_topology_snapshot

# Exact frozen A1 fixture specification, embedded for portable CI execution.
_ORACLES = json.loads(r"""{
  "status": "STATIC A1 SPECIFICATION; NOT EXECUTED C1/B1/H1 TEST RESULTS",
  "as_of": "2026-11-21T00:01:00Z",
  "captured_at": "2026-11-21T00:00:00Z",
  "default_policy": "section 4 of A1 decision; unspecified overrides use defaults",
  "graphs": {
    "chain": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002",
        "C": "00000000-0000-0000-0000-000000000003",
        "D": "00000000-0000-0000-0000-000000000004"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "B",
          "C",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api",
        "C": "api",
        "D": "api"
      },
      "topology_id": "7fe76dc2-9a34-5778-b772-4dc172544469",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "b4e9f32fd32a535699c5d8cea07984d85724e38432946a116b3ecbc4c6b705c0"
    },
    "isolated": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001"
      },
      "edges": [],
      "service_types": {
        "A": "api"
      },
      "topology_id": "9ce57050-b95a-5f5c-b317-a7f63ef2fd27",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "177dc54b2c164680906d5304f6ca94d2badf4117a6c36a65bbcc86bbc210eab0"
    },
    "cycle": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002",
        "C": "00000000-0000-0000-0000-000000000003"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "B",
          "C",
          "sync"
        ],
        [
          "C",
          "A",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api",
        "C": "api"
      },
      "topology_id": "52e4fe27-d596-5dc8-bf48-77512f990e4a",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "61b74302407103b2c09099948a2d71d3750ccc0fa9fceaef9d4ba588e3b1ac06"
    },
    "diamond": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002",
        "C": "00000000-0000-0000-0000-000000000003",
        "D": "00000000-0000-0000-0000-000000000004"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "A",
          "C",
          "sync"
        ],
        [
          "B",
          "D",
          "sync"
        ],
        [
          "C",
          "D",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api",
        "C": "api",
        "D": "api"
      },
      "topology_id": "baf6c09e-1009-5ef8-b216-064795ec0bec",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "142ff3a20191860cc7ce37da508bada47e416c4ee1d4b64a9f65bb64e6a256c0"
    },
    "multi_root_chain": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002",
        "C": "00000000-0000-0000-0000-000000000003",
        "D": "00000000-0000-0000-0000-000000000004"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "B",
          "C",
          "sync"
        ],
        [
          "C",
          "D",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api",
        "C": "api",
        "D": "api"
      },
      "topology_id": "551877aa-6b98-5a7d-9565-4e2ef12cd847",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "0a7d05ac8550499bde4ce76d89b5561edfa2a35491bd83c8b99e1397386f16e0"
    },
    "parallel": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002"
      },
      "edges": [
        [
          "A",
          "B",
          "data"
        ],
        [
          "A",
          "B",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api"
      },
      "topology_id": "5d43459c-8628-5b77-a8be-4a8452cd144a",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "d5fdb5031759ce8e6d72c3c916bedda6cacab2c78e64b573d0e092f6bc3b6e3e"
    },
    "canonical": {
      "ids": {
        "client": "29fb0a3b-31a1-527b-9ecf-b50cba780338",
        "api-gateway": "a423c462-9e3d-5df7-9b88-2a71637a958f",
        "order-service": "fdface04-c1df-5daa-ab47-23debcebb2fd",
        "payment-service": "2b8e9c49-61b0-5a53-821d-6193136b1db2",
        "inventory-service": "936fb446-a6f1-5cf9-88d2-65789b6e6eae",
        "notification-service": "10ed11a8-4faf-50f5-9903-2417c842278c",
        "database": "a50eab12-f3a8-544a-be74-060c56b3e254"
      },
      "service_types": {
        "client": "external",
        "api-gateway": "api",
        "order-service": "api",
        "payment-service": "api",
        "inventory-service": "api",
        "notification-service": "worker",
        "database": "database"
      },
      "topology_id": "77bb041d-fb4c-5370-9b99-45bef0082479",
      "version": "1.0.0",
      "edges": [
        [
          "client",
          "api-gateway",
          "sync"
        ],
        [
          "api-gateway",
          "order-service",
          "sync"
        ],
        [
          "order-service",
          "payment-service",
          "sync"
        ],
        [
          "payment-service",
          "database",
          "data"
        ],
        [
          "order-service",
          "inventory-service",
          "sync"
        ],
        [
          "inventory-service",
          "database",
          "data"
        ],
        [
          "order-service",
          "notification-service",
          "async"
        ]
      ],
      "snapshot_fingerprint": "2d9f8e9a54a8d1ce103de39be842b4fcfa8e36b32b41f4f0d7abc1820d6779dd"
    },
    "disconnected": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002",
        "C": "00000000-0000-0000-0000-000000000003",
        "D": "00000000-0000-0000-0000-000000000004"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "C",
          "D",
          "sync"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api",
        "C": "api",
        "D": "api"
      },
      "topology_id": "48f68f22-4959-5a6e-b634-b5155f411560",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "b410d360bcae785f1f2067a74d80a791c120396a00a71b8cd82eb28f7ec5180d"
    },
    "opposite": {
      "ids": {
        "A": "00000000-0000-0000-0000-000000000001",
        "B": "00000000-0000-0000-0000-000000000002"
      },
      "edges": [
        [
          "A",
          "B",
          "sync"
        ],
        [
          "B",
          "A",
          "data"
        ]
      ],
      "service_types": {
        "A": "api",
        "B": "api"
      },
      "topology_id": "61859f10-1bc0-5def-9222-deab531e5a63",
      "version": "fixture/1.0",
      "snapshot_fingerprint": "dcdf4d8566b96de8f05452cb43ba895a6f5bf34d55e56f407c2168f08be71593"
    }
  },
  "positive_cases": [
    {
      "id": "F01",
      "graph": "canonical",
      "roots": [
        "order-service"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "notification-service",
          "payment-service",
          "inventory-service",
          "database",
          "order-service"
        ],
        "depths": {
          "notification-service": 1,
          "payment-service": 1,
          "inventory-service": 1,
          "database": 2,
          "order-service": 0
        },
        "witness_paths": {
          "order-service": [
            "order-service"
          ],
          "payment-service": [
            "order-service",
            "payment-service"
          ],
          "inventory-service": [
            "order-service",
            "inventory-service"
          ],
          "notification-service": [
            "order-service",
            "notification-service"
          ],
          "database": [
            "order-service",
            "payment-service",
            "database"
          ]
        },
        "graph_edges": [
          [
            "payment-service",
            "database",
            "data"
          ],
          [
            "inventory-service",
            "database",
            "data"
          ],
          [
            "order-service",
            "notification-service",
            "async"
          ],
          [
            "order-service",
            "payment-service",
            "sync"
          ],
          [
            "order-service",
            "inventory-service",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 5,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "order-service": [],
          "payment-service": [
            "sync"
          ],
          "inventory-service": [
            "sync"
          ],
          "notification-service": [
            "async"
          ],
          "database": [
            "sync",
            "data"
          ]
        }
      }
    },
    {
      "id": "F02",
      "graph": "canonical",
      "roots": [
        "payment-service"
      ],
      "max_hops": 2,
      "direction": "upstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "payment-service",
          "api-gateway",
          "order-service"
        ],
        "depths": {
          "payment-service": 0,
          "api-gateway": 2,
          "order-service": 1
        },
        "witness_paths": {
          "payment-service": [
            "payment-service"
          ],
          "order-service": [
            "payment-service",
            "order-service"
          ],
          "api-gateway": [
            "payment-service",
            "order-service",
            "api-gateway"
          ]
        },
        "graph_edges": [
          [
            "api-gateway",
            "order-service",
            "sync"
          ],
          [
            "order-service",
            "payment-service",
            "sync"
          ]
        ],
        "truncated": true,
        "truncation_reasons": [
          "hop_limit"
        ],
        "examined_arcs": 3,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "payment-service": [],
          "order-service": [
            "sync"
          ],
          "api-gateway": [
            "sync",
            "sync"
          ]
        }
      }
    },
    {
      "id": "F03",
      "graph": "canonical",
      "roots": [
        "order-service"
      ],
      "max_hops": 1,
      "direction": "both",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "notification-service",
          "payment-service",
          "inventory-service",
          "api-gateway",
          "order-service"
        ],
        "depths": {
          "notification-service": 1,
          "payment-service": 1,
          "inventory-service": 1,
          "api-gateway": 1,
          "order-service": 0
        },
        "witness_paths": {
          "order-service": [
            "order-service"
          ],
          "api-gateway": [
            "order-service",
            "api-gateway"
          ],
          "payment-service": [
            "order-service",
            "payment-service"
          ],
          "inventory-service": [
            "order-service",
            "inventory-service"
          ],
          "notification-service": [
            "order-service",
            "notification-service"
          ]
        },
        "graph_edges": [
          [
            "api-gateway",
            "order-service",
            "sync"
          ],
          [
            "order-service",
            "notification-service",
            "async"
          ],
          [
            "order-service",
            "payment-service",
            "sync"
          ],
          [
            "order-service",
            "inventory-service",
            "sync"
          ]
        ],
        "truncated": true,
        "truncation_reasons": [
          "hop_limit"
        ],
        "examined_arcs": 11,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "order-service": [],
          "api-gateway": [
            "sync"
          ],
          "payment-service": [
            "sync"
          ],
          "inventory-service": [
            "sync"
          ],
          "notification-service": [
            "async"
          ]
        }
      }
    },
    {
      "id": "F04",
      "graph": "isolated",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A"
        ],
        "depths": {
          "A": 0
        },
        "witness_paths": {
          "A": [
            "A"
          ]
        },
        "graph_edges": [],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 0,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": []
        }
      }
    },
    {
      "id": "F05",
      "graph": "cycle",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 2
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "A",
            "B",
            "C"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "B",
            "C",
            "sync"
          ],
          [
            "C",
            "A",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 3,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [
            "sync",
            "sync"
          ]
        }
      }
    },
    {
      "id": "F06",
      "graph": "diamond",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C",
          "D"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 1,
          "D": 2
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "A",
            "C"
          ],
          "D": [
            "A",
            "B",
            "D"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "A",
            "C",
            "sync"
          ],
          [
            "B",
            "D",
            "sync"
          ],
          [
            "C",
            "D",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 4,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [
            "sync"
          ],
          "D": [
            "sync",
            "sync"
          ]
        }
      }
    },
    {
      "id": "F07",
      "graph": "multi_root_chain",
      "roots": [
        "A",
        "C"
      ],
      "max_hops": 1,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C",
          "D"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 0,
          "D": 1
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "C"
          ],
          "D": [
            "C",
            "D"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "B",
            "C",
            "sync"
          ],
          [
            "C",
            "D",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 3,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [],
          "D": [
            "sync"
          ]
        }
      }
    },
    {
      "id": "F08",
      "graph": "parallel",
      "roots": [
        "A"
      ],
      "max_hops": 1,
      "direction": "both",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B"
        ],
        "depths": {
          "A": 0,
          "B": 1
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "data"
          ],
          [
            "A",
            "B",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 4,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": [],
          "B": [
            "data"
          ]
        }
      }
    },
    {
      "id": "F09",
      "graph": "chain",
      "roots": [
        "A"
      ],
      "max_hops": 0,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A"
        ],
        "depths": {
          "A": 0
        },
        "witness_paths": {
          "A": [
            "A"
          ]
        },
        "graph_edges": [],
        "truncated": true,
        "truncation_reasons": [
          "hop_limit"
        ],
        "examined_arcs": 1,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": []
        }
      }
    },
    {
      "id": "F10",
      "graph": "diamond",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {
        "max_services": 3
      },
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 1
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "A",
            "C"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "A",
            "C",
            "sync"
          ]
        ],
        "truncated": true,
        "truncation_reasons": [
          "service_limit"
        ],
        "examined_arcs": 4,
        "omitted_within_hop_services": 1,
        "omitted_eligible_edges": 2,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [
            "sync"
          ]
        }
      }
    },
    {
      "id": "F11",
      "graph": "diamond",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {
        "max_edges": 3
      },
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C",
          "D"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 1,
          "D": 2
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "A",
            "C"
          ],
          "D": [
            "A",
            "B",
            "D"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "A",
            "C",
            "sync"
          ],
          [
            "B",
            "D",
            "sync"
          ]
        ],
        "truncated": true,
        "truncation_reasons": [
          "edge_limit"
        ],
        "examined_arcs": 4,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 1,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [
            "sync"
          ],
          "D": [
            "sync",
            "sync"
          ]
        }
      }
    },
    {
      "id": "F12",
      "graph": "canonical",
      "roots": [
        "order-service"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {
        "allowed_edge_types": [
          "async"
        ]
      },
      "expected": {
        "services_in_UUID_order": [
          "notification-service",
          "order-service"
        ],
        "depths": {
          "notification-service": 1,
          "order-service": 0
        },
        "witness_paths": {
          "order-service": [
            "order-service"
          ],
          "notification-service": [
            "order-service",
            "notification-service"
          ]
        },
        "graph_edges": [
          [
            "order-service",
            "notification-service",
            "async"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 1,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "order-service": [],
          "notification-service": [
            "async"
          ]
        }
      }
    },
    {
      "id": "F13",
      "graph": "chain",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {
        "max_examined_arcs": 2
      },
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B",
          "C"
        ],
        "depths": {
          "A": 0,
          "B": 1,
          "C": 2
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ],
          "C": [
            "A",
            "B",
            "C"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "B",
            "C",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 2,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0,
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ],
          "C": [
            "sync",
            "sync"
          ]
        }
      }
    },
    {
      "id": "F14",
      "graph": "disconnected",
      "roots": [
        "A"
      ],
      "max_hops": 2,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B"
        ],
        "depths": {
          "A": 0,
          "B": 1
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ]
        },
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 1,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0
      }
    },
    {
      "id": "F15",
      "graph": "diamond",
      "roots": [
        "B",
        "C"
      ],
      "max_hops": 1,
      "direction": "downstream",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "B",
          "C",
          "D"
        ],
        "depths": {
          "B": 0,
          "C": 0,
          "D": 1
        },
        "witness_paths": {
          "B": [
            "B"
          ],
          "C": [
            "C"
          ],
          "D": [
            "B",
            "D"
          ]
        },
        "witness_edge_types": {
          "B": [],
          "C": [],
          "D": [
            "sync"
          ]
        },
        "graph_edges": [
          [
            "B",
            "D",
            "sync"
          ],
          [
            "C",
            "D",
            "sync"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 2,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0
      }
    },
    {
      "id": "F16",
      "graph": "opposite",
      "roots": [
        "A"
      ],
      "max_hops": 1,
      "direction": "both",
      "policy_overrides": {},
      "expected": {
        "services_in_UUID_order": [
          "A",
          "B"
        ],
        "depths": {
          "A": 0,
          "B": 1
        },
        "witness_paths": {
          "A": [
            "A"
          ],
          "B": [
            "A",
            "B"
          ]
        },
        "witness_edge_types": {
          "A": [],
          "B": [
            "sync"
          ]
        },
        "graph_edges": [
          [
            "A",
            "B",
            "sync"
          ],
          [
            "B",
            "A",
            "data"
          ]
        ],
        "truncated": false,
        "truncation_reasons": [],
        "examined_arcs": 4,
        "omitted_within_hop_services": 0,
        "omitted_eligible_edges": 0
      }
    }
  ],
  "rejection_cases": [
    {
      "id": "R01",
      "graph": "chain",
      "input_change": "max_examined_arcs=1; H=2",
      "expected_error": "BlastRadiusLimitError",
      "no_output": true
    },
    {
      "id": "R02",
      "graph": "diamond",
      "input_change": "max_edges=2; H=2 requires three witness edges",
      "expected_error": "BlastRadiusLimitError",
      "no_output": true
    },
    {
      "id": "R03",
      "graph": "multi_root_chain",
      "input_change": "roots A,C; max_services=1",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R04",
      "graph": "canonical",
      "input_change": "unknown member name",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R05",
      "graph": "canonical",
      "input_change": "two snapshot UUIDs share a seed name",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R06",
      "graph": "canonical",
      "input_change": "wrong expected topology ID",
      "expected_error": "BlastRadiusTopologyMismatchError",
      "no_output": true
    },
    {
      "id": "R07",
      "graph": "canonical",
      "input_change": "wrong expected version",
      "expected_error": "BlastRadiusTopologyMismatchError",
      "no_output": true
    },
    {
      "id": "R08",
      "graph": "canonical",
      "input_change": "same version, different independently expected fingerprint",
      "expected_error": "BlastRadiusTopologyMismatchError",
      "no_output": true
    },
    {
      "id": "R09",
      "graph": "canonical",
      "input_change": "missing independent pin with incident.topology_snapshot=None",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R10",
      "graph": "canonical",
      "input_change": "captured_at later than explicit as_of",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R11",
      "graph": "canonical",
      "input_change": "age 61 seconds with max_snapshot_age_seconds=60",
      "expected_error": "BlastRadiusTopologyMismatchError",
      "no_output": true
    },
    {
      "id": "R12",
      "graph": "canonical",
      "input_change": "boolean instead of integer limit",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R13",
      "graph": "canonical",
      "input_change": "max_hops=33",
      "expected_error": "BlastRadiusInputError",
      "no_output": true
    },
    {
      "id": "R14",
      "graph": "canonical",
      "input_change": "context UTF-8 encoded size=2097153",
      "expected_error": "BlastRadiusLimitError",
      "no_output": true
    },
    {
      "id": "R15",
      "graph": "canonical",
      "input_change": "handoff UTF-8 encoded size=67108865",
      "expected_error": "Phase5HandoffContractError",
      "no_output": true
    },
    {
      "id": "R16",
      "graph": "canonical",
      "input_change": "extra RCA/remediation field at a new nested object boundary",
      "expected_error": "Phase5HandoffContractError",
      "no_output": true
    },
    {
      "id": "R17",
      "graph": "canonical",
      "input_change": "altered depth/path with recomputed self-consistent hashes supplied to semantic validator",
      "expected_error": "Phase5HandoffContractError",
      "no_output": true
    },
    {
      "id": "R18",
      "graph": "canonical",
      "input_change": "duplicate JSON key on envelope decode",
      "expected_error": "Phase5HandoffContractError",
      "no_output": true
    }
  ],
  "boundary_acceptances": [
    "age exactly 60s with max_snapshot_age_seconds=60",
    "strict max_hops 0 and 32",
    "exact 512 returned nodes/paths; 1024 returned edges if witnesses fit",
    "exact 2097152 context bytes and 67108864 envelope bytes; no mandatory data removal",
    "all roots retained and root count exactly512; reject513",
    "permutation of members/nodes/edges preserves canonical bytes",
    "unchanged legacy aggregate bytes/fingerprint including None topology fields after handoff roundtrip"
  ],
  "notes": [
    "Synthetic isolated graph F04 has only root A. In disconnected testing, add disconnected nodes/edges and require identical reachable answer; graph pin and context hash must change to bind the different full snapshot.",
    "Static answers were enumerated from directed edges, not produced by a traversal routine. UUID sorting and SHA-256 assembly are formatting/identity calculations only.",
    "Context/envelope final fingerprints are computed by C1 from these fixed graph answers, exact source incident and policy; snapshot pins above are fixed golden values."
  ]
}
""")


def context_payload(case_id="F01"):
    case = next(c for c in _ORACLES["positive_cases"] if c["id"] == case_id)
    graph, expected = _ORACLES["graphs"][case["graph"]], case["expected"]
    ids = graph["ids"]

    def edge(e):
        return dict(
            upstream_service_id=ids[e[0]],
            downstream_service_id=ids[e[1]],
            dependency_type=e[2],
        )

    policy = dict(
        policy_version="blast-radius-policy/1.0",
        direction=case["direction"],
        max_hops=case["max_hops"],
        allowed_edge_types=["async", "control", "data", "generic", "sync"],
        max_services=512,
        max_edges=1024,
        max_examined_arcs=65536,
        max_snapshot_age_seconds=None,
    )
    policy.update(case["policy_overrides"])
    paths = []
    for target in expected["services_in_UUID_order"]:
        chain = expected["witness_paths"][target]
        path_edges = []
        for a, b, t in zip(chain, chain[1:], expected["witness_edge_types"][target]):
            candidates = [
                e
                for e in graph["edges"]
                if e[2] == t
                and (
                    e[:2] == [a, b]
                    or (case["direction"] in ["upstream", "both"] and e[:2] == [b, a])
                )
            ]
            chosen = min(candidates, key=lambda e: (ids[e[0]], ids[e[1]], e[2]))
            path_edges.append(edge(chosen))
        paths.append(
            dict(
                root_service_id=ids[chain[0]],
                target_service_id=ids[target],
                direction=case["direction"],
                service_ids=[ids[n] for n in chain],
                edges=path_edges,
                hop_count=len(chain) - 1,
            )
        )
    p = dict(
        schema_version="blast-radius-context/1.0",
        policy=policy,
        topology_snapshot=dict(
            topology_id=graph["topology_id"],
            topology_version=graph["version"],
            snapshot_fingerprint=graph["snapshot_fingerprint"],
            captured_at=_ORACLES["captured_at"],
            provider="canonical",
        ),
        member_services=[dict(service_id=ids[n], name=n) for n in case["roots"]],
        reachable_services=[
            dict(
                service_id=ids[n],
                name=n,
                minimum_depth=expected["depths"][n],
                witness_root_service_id=ids[expected["witness_paths"][n][0]],
            )
            for n in expected["services_in_UUID_order"]
        ],
        edges=[edge(e) for e in expected["graph_edges"]],
        paths=paths,
        potentially_affected_service_ids=[
            ids[n] for n in expected["services_in_UUID_order"]
        ],
        truncated=expected["truncated"],
        truncation_reasons=expected["truncation_reasons"],
        omitted_within_hop_services=expected["omitted_within_hop_services"],
        omitted_eligible_edges=expected["omitted_eligible_edges"],
        examined_arcs=expected["examined_arcs"],
    )
    p["context_fingerprint"] = compute_context_fingerprint(p)
    return p


def encode(p):
    return json.dumps(
        p, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def signed(p):
    p["context_fingerprint"] = compute_context_fingerprint(p)
    return encode(p)


@pytest.mark.parametrize("case_id", [c["id"] for c in _ORACLES["positive_cases"]])
def test_a1_context_fixture_roundtrip(case_id):
    p = context_payload(case_id)
    before = encode(p)
    context = decode_blast_radius_context_structural(before)
    assert serialize_blast_radius_context(context) == before
    assert context.model_dump(mode="json") == p
    assert (
        context.context_fingerprint
        == hashlib.sha256(
            encode({k: v for k, v in p.items() if k != "context_fingerprint"})
        ).hexdigest()
    )
    assert encode(p) == before


@pytest.mark.parametrize("graph_name", list(_ORACLES["graphs"]))
def test_static_snapshot_pins_use_existing_topology_contract(graph_name):
    g = _ORACLES["graphs"][graph_name]
    ids = g["ids"]
    raw = dict(
        schema_version="1.0",
        version=dict(topology_id=g["topology_id"], topology_version=g["version"]),
        nodes=[
            dict(service_id=v, name=k, service_type=g["service_types"][k])
            for k, v in ids.items()
        ],
        edges=[
            dict(
                upstream_service_id=ids[a],
                downstream_service_id=ids[b],
                dependency_type=t,
            )
            for a, b, t in g["edges"]
        ],
        snapshot_fingerprint=g["snapshot_fingerprint"],
    )
    snapshot = TopologySnapshot.model_validate_json(encode(raw))
    before = serialize_topology_snapshot(snapshot)
    decode_blast_radius_context_structural(signed(context_payload("F01")))
    assert serialize_topology_snapshot(snapshot) == before


@pytest.mark.parametrize(
    "field",
    [
        "member_services",
        "reachable_services",
        "paths",
        "edges",
        "potentially_affected_service_ids",
        "truncation_reasons",
    ],
)
def test_collection_permutations_preserve_context_bytes(field):
    p = context_payload("F10" if field == "truncation_reasons" else "F01")
    original = encode(p)
    p[field].reverse()
    p["policy"]["allowed_edge_types"].reverse()
    assert (
        serialize_blast_radius_context(
            decode_blast_radius_context_structural(encode(p))
        )
        == original
    )


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("policy",),
        ("topology_snapshot",),
        ("member_services", 0),
        ("reachable_services", 0),
        ("paths", 0),
        ("edges", 0),
        ("paths", 0, "edges", 0),
    ],
)
def test_forbidden_fields_rejected_at_every_new_boundary(path):
    p = context_payload()
    obj = p
    for key in path:
        obj = obj[key]
    obj["root_cause_candidate"] = "unsupported"
    with pytest.raises(BlastRadiusInputError):
        decode_blast_radius_context_structural(signed(p))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("max_hops", True),
        ("max_hops", -1),
        ("max_hops", 33),
        ("max_hops", 2.0),
        ("max_services", 0),
        ("max_services", 513),
        ("max_edges", -1),
        ("max_edges", 1025),
        ("max_examined_arcs", 0),
        ("max_examined_arcs", 65537),
        ("max_snapshot_age_seconds", -1),
        ("max_snapshot_age_seconds", 31536001),
        ("max_snapshot_age_seconds", True),
        ("direction", "sideways"),
        ("allowed_edge_types", []),
        ("allowed_edge_types", ["sync", "sync"]),
        ("policy_version", "v2"),
    ],
)
def test_policy_invalid_values(field, bad):
    p = context_payload()
    p["policy"][field] = bad
    with pytest.raises(BlastRadiusInputError):
        decode_blast_radius_context_structural(signed(p))


@pytest.mark.parametrize(
    "field,boundaries",
    [
        ("max_hops", [0, 32]),
        ("max_services", [1, 512]),
        ("max_edges", [0, 1024]),
        ("max_examined_arcs", [1, 65536]),
        ("max_snapshot_age_seconds", [0, 31536000]),
    ],
)
def test_policy_exact_integer_boundaries(field, boundaries):
    for value in boundaries:
        p = BlastRadiusPolicy.model_validate_json(encode({field: value}))
        assert getattr(p, field) == value


@pytest.mark.parametrize(
    "mutation",
    [
        "duplicate_root",
        "missing_path",
        "wrong_depth",
        "wrong_root",
        "wrong_endpoint",
        "missing_witness_edge",
        "wrong_affected",
        "duplicate_edge",
        "false_truncated",
        "unknown_reason",
        "bad_counter",
        "counter_bool",
        "omitted_without_reason",
        "bad_hash",
        "future_schema",
        "too_few_services",
        "too_few_edges",
        "work_cap",
        "bad_name",
        "bad_fingerprint_format",
    ],
)
def test_inconsistent_context_rejected_even_when_rehashed(mutation):
    p = context_payload()
    if mutation == "duplicate_root":
        p["member_services"] *= 2
    elif mutation == "missing_path":
        p["paths"].pop()
    elif mutation == "wrong_depth":
        p["reachable_services"][0]["minimum_depth"] = 31
    elif mutation == "wrong_root":
        p["paths"][0]["root_service_id"] = str(UUID(int=99))
    elif mutation == "wrong_endpoint":
        p["paths"][0]["target_service_id"] = str(UUID(int=99))
    elif mutation == "missing_witness_edge":
        p["edges"] = []
    elif mutation == "wrong_affected":
        p["potentially_affected_service_ids"].pop()
    elif mutation == "duplicate_edge":
        p["edges"].append(p["edges"][0])
    elif mutation == "false_truncated":
        p["truncated"] = True
    elif mutation == "unknown_reason":
        p["truncation_reasons"] = ["unknown"]
    elif mutation == "bad_counter":
        p["examined_arcs"] = 65537
    elif mutation == "counter_bool":
        p["examined_arcs"] = True
    elif mutation == "omitted_without_reason":
        p["omitted_eligible_edges"] = 1
    elif mutation == "bad_hash":
        p["context_fingerprint"] = "a" * 64
    elif mutation == "future_schema":
        p["schema_version"] = "blast-radius-context/2.0"
    elif mutation == "too_few_services":
        p["policy"]["max_services"] = 1
    elif mutation == "too_few_edges":
        p["policy"]["max_edges"] = 0
    elif mutation == "work_cap":
        p["policy"]["max_examined_arcs"] = 1
    elif mutation == "bad_name":
        p["member_services"][0]["name"] = " "
    elif mutation == "bad_fingerprint_format":
        p["topology_snapshot"]["snapshot_fingerprint"] = "a" * 64 + "\n"
    raw = encode(p) if mutation == "bad_hash" else signed(p)
    with pytest.raises(BlastRadiusInputError):
        decode_blast_radius_context_structural(raw)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"x":1,"x":2}',
        b'{"x":{"a":1,"a":2}}',
        b'{"x":NaN}',
        b"[]",
        b"\xff",
        b"not-json",
    ],
)
def test_invalid_json_and_duplicate_keys_fail(raw):
    with pytest.raises(BlastRadiusInputError):
        decode_blast_radius_context_structural(raw)


def test_byte_bounds_count_utf8_and_check_raw_before_parse():
    p = {"name": "\u03bb"}
    raw = encode(p)
    assert _canonical_bytes(p, len(raw)) == raw
    with pytest.raises(BlastRadiusLimitError):
        _canonical_bytes(p, len(raw) - 1)
    assert _decode_object(raw, len(raw)) == p
    with pytest.raises(BlastRadiusLimitError):
        _decode_object(raw + b" ", len(raw))
    with pytest.raises(BlastRadiusLimitError):
        decode_blast_radius_context_structural(b" " * (MAX_CONTEXT_BYTES + 1))
    assert MAX_HANDOFF_BYTES == 67108864 and MAX_CONTEXT_BYTES == 2097152


def test_unsafe_copied_nested_instances_revalidated_and_frozen():
    original = decode_blast_radius_context_structural(signed(context_payload()))
    with pytest.raises(ValidationError):
        original.truncated = True
    bad = original.model_copy(
        update={"policy": original.policy.model_copy(update={"max_hops": True})}
    )
    with pytest.raises(BlastRadiusInputError):
        serialize_blast_radius_context(bad)
    invalid_edge = original.edges[0].model_copy(
        update={"downstream_service_id": original.edges[0].upstream_service_id}
    )
    with pytest.raises(BlastRadiusInputError):
        serialize_blast_radius_context(
            original.model_copy(update={"edges": (invalid_edge,)})
        )


@pytest.mark.parametrize(
    "location", ["context", "policy", "member", "path", "edge", "reference"]
)
def test_unsafe_copy_unknown_fields_are_rejected_not_silently_dropped(location):
    original = decode_blast_radius_context_structural(signed(context_payload()))
    update = {"remediation": "unsupported"}
    if location == "context":
        bad = original.model_copy(update=update)
    elif location == "policy":
        bad = original.model_copy(
            update={"policy": original.policy.model_copy(update=update)}
        )
    elif location == "member":
        bad = original.model_copy(
            update={
                "member_services": (
                    original.member_services[0].model_copy(update=update),
                )
            }
        )
    elif location == "path":
        bad = original.model_copy(
            update={"paths": (original.paths[0].model_copy(update=update),)}
        )
    elif location == "edge":
        bad = original.model_copy(
            update={"edges": (original.edges[0].model_copy(update=update),)}
        )
    else:
        bad = original.model_copy(
            update={
                "topology_snapshot": original.topology_snapshot.model_copy(
                    update=update
                )
            }
        )
    with pytest.raises(BlastRadiusInputError):
        serialize_blast_radius_context(bad)


def test_exact_context_raw_ceiling_then_one_byte_over():
    raw = signed(context_payload())
    exact = raw + b" " * (MAX_CONTEXT_BYTES - len(raw))
    assert (
        serialize_blast_radius_context(decode_blast_radius_context_structural(exact))
        == raw
    )
    with pytest.raises(BlastRadiusLimitError):
        decode_blast_radius_context_structural(exact + b" ")


# B1 calculator tests consume the static A1 answers above, never calculate
# their own expected paths using the production traversal implementation.
def _incident_for_services(names):
    from app.correlation.models import IncidentAggregate
    from test_correlation_handoff import incident_fixture

    base = incident_fixture()
    fields = base.model_dump(mode="python", exclude={"revision_fingerprint"})
    fields["signal_references"] = tuple(
        base.signal_references[0].model_copy(
            update={
                "signal_id": UUID(int=1000 + i),
                "semantic_fingerprint": hashlib.sha256(str(i).encode()).hexdigest(),
                "service": name,
            }
        )
        for i, name in enumerate(names)
    )
    fields.update(
        primary_service=names[0],
        affected_services=tuple(sorted(set(names)))[:512],
        correlation_evidence=(),
    )
    return IncidentAggregate.create(**fields)


def _calculator_inputs(case_id="F01"):
    from app.correlation.models import TopologySnapshotReference

    c = next(c for c in _ORACLES["positive_cases"] if c["id"] == case_id)
    g = _ORACLES["graphs"][c["graph"]]
    ids = g["ids"]
    snapshot = TopologySnapshot.model_validate_json(
        encode(
            dict(
                schema_version="1.0",
                version=dict(
                    topology_id=g["topology_id"], topology_version=g["version"]
                ),
                nodes=[
                    dict(
                        service_id=sid, name=name, service_type=g["service_types"][name]
                    )
                    for name, sid in ids.items()
                ],
                edges=[
                    dict(
                        upstream_service_id=ids[a],
                        downstream_service_id=ids[b],
                        dependency_type=t,
                    )
                    for a, b, t in g["edges"]
                ],
                snapshot_fingerprint=g["snapshot_fingerprint"],
            )
        )
    )
    p = context_payload(case_id)
    return dict(
        incident=_incident_for_services(c["roots"]),
        snapshot=snapshot,
        expected_topology=TopologySnapshotReference.model_validate_json(
            encode(p["topology_snapshot"])
        ),
        policy=BlastRadiusPolicy.model_validate_json(encode(p["policy"])),
        as_of=__import__("datetime").datetime.fromisoformat(
            _ORACLES["as_of"].replace("Z", "+00:00")
        ),
    )


@pytest.mark.parametrize("case_id", [c["id"] for c in _ORACLES["positive_cases"]])
def test_calculator_matches_every_a1_fixture_field(case_id):
    from app.correlation.blast_radius import calculate_blast_radius
    from app.correlation.serialization import serialize_incident_aggregate

    args = _calculator_inputs(case_id)
    before = (
        serialize_incident_aggregate(args["incident"]),
        serialize_topology_snapshot(args["snapshot"]),
        args["policy"].model_dump_json(),
        args["expected_topology"].model_dump_json(),
    )
    result = calculate_blast_radius(**args)
    assert result.model_dump(mode="json") == context_payload(case_id)
    assert serialize_blast_radius_context(result) == encode(context_payload(case_id))
    assert before == (
        serialize_incident_aggregate(args["incident"]),
        serialize_topology_snapshot(args["snapshot"]),
        args["policy"].model_dump_json(),
        args["expected_topology"].model_dump_json(),
    )


@pytest.mark.parametrize("case_id", ["F01", "F06", "F07", "F08", "F15", "F16"])
def test_calculator_permutations_and_repetition_preserve_bytes(case_id):
    from app.correlation.blast_radius import calculate_blast_radius

    args = _calculator_inputs(case_id)
    expected = serialize_blast_radius_context(calculate_blast_radius(**args))
    snapshot = args["snapshot"].model_copy(
        update={
            "nodes": tuple(reversed(args["snapshot"].nodes)),
            "edges": tuple(reversed(args["snapshot"].edges)),
        }
    )
    args["snapshot"] = snapshot
    before = serialize_topology_snapshot(snapshot)
    for _ in range(3):
        assert (
            serialize_blast_radius_context(calculate_blast_radius(**args)) == expected
        )
    assert serialize_topology_snapshot(snapshot) == before


@pytest.mark.parametrize(
    "change,error",
    [
        ("pin_id", "mismatch"),
        ("pin_version", "mismatch"),
        ("pin_hash", "mismatch"),
        ("missing_pin", "input"),
        ("incident_pin", "mismatch"),
        ("future_capture", "input"),
        ("expired", "mismatch"),
        ("as_of_naive", "input"),
        ("as_of_non_utc", "input"),
        ("as_of_wrong_type", "input"),
        ("unknown_member", "input"),
        ("ambiguous_member", "input"),
        ("member_whitespace", "input"),
        ("primary_not_member", "input"),
        ("invalid_policy", "input"),
        ("budget_work", "limit"),
        ("budget_edge", "limit"),
        ("budget_roots", "input"),
        ("corrupt_snapshot", "input"),
        ("copied_bool_revision", "input"),
        ("copied_extra_incident", "input"),
    ],
)
def test_calculator_rejects_invalid_inputs_with_designated_error(change, error):
    from datetime import datetime, timedelta, timezone
    from app.correlation.blast_radius import (
        calculate_blast_radius,
        BlastRadiusTopologyMismatchError,
    )
    from app.correlation.models import IncidentAggregate
    from app.correlation.topology import TopologyServiceNode

    args = _calculator_inputs(
        "F06"
        if change == "budget_edge"
        else "F07"
        if change == "budget_roots"
        else "F13"
        if change == "budget_work"
        else "F01"
    )
    expected = {
        "mismatch": BlastRadiusTopologyMismatchError,
        "input": BlastRadiusInputError,
        "limit": BlastRadiusLimitError,
    }[error]
    if change == "pin_id":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"topology_id": UUID(int=99)}
        )
    elif change == "pin_version":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"topology_version": "stale"}
        )
    elif change == "pin_hash":
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"snapshot_fingerprint": "a" * 64}
        )
    elif change == "missing_pin":
        args["expected_topology"] = None
    elif change == "incident_pin":
        fields = args["incident"].model_dump(
            mode="python", exclude={"revision_fingerprint"}
        )
        fields["topology_snapshot"] = args["expected_topology"].model_copy(
            update={"topology_version": "stale"}
        )
        args["incident"] = IncidentAggregate.create(**fields)
    elif change == "future_capture":
        args["as_of"] = args["expected_topology"].captured_at - timedelta(seconds=1)
    elif change == "expired":
        args["policy"] = args["policy"].model_copy(
            update={"max_snapshot_age_seconds": 59}
        )
    elif change == "as_of_naive":
        args["as_of"] = datetime(2026, 11, 21)
    elif change == "as_of_non_utc":
        args["as_of"] = datetime(2026, 11, 21, tzinfo=timezone(timedelta(hours=1)))
    elif change == "as_of_wrong_type":
        args["as_of"] = True
    elif change == "unknown_member":
        args["incident"] = _incident_for_services(["unknown"])
    elif change == "ambiguous_member":
        extra = TopologyServiceNode.model_validate_json(
            encode(
                dict(
                    service_id=str(UUID(int=99)),
                    name="order-service",
                    service_type="api",
                )
            )
        )
        args["snapshot"] = TopologySnapshot.create(
            version=args["snapshot"].version,
            nodes=(*args["snapshot"].nodes, extra),
            edges=args["snapshot"].edges,
        )
        args["expected_topology"] = args["expected_topology"].model_copy(
            update={"snapshot_fingerprint": args["snapshot"].snapshot_fingerprint}
        )
    elif change == "member_whitespace":
        refs = args["incident"].signal_references
        args["incident"] = args["incident"].model_copy(
            update={
                "signal_references": (
                    refs[0].model_copy(update={"service": " order-service"}),
                )
            }
        )
    elif change == "primary_not_member":
        fields = args["incident"].model_dump(
            mode="python", exclude={"revision_fingerprint"}
        )
        fields.update(
            primary_service="other", affected_services=("order-service", "other")
        )
        args["incident"] = IncidentAggregate.create(**fields)
    elif change == "invalid_policy":
        args["policy"] = args["policy"].model_copy(update={"max_hops": True})
    elif change == "budget_work":
        args["policy"] = args["policy"].model_copy(update={"max_examined_arcs": 1})
    elif change == "budget_edge":
        args["policy"] = args["policy"].model_copy(update={"max_edges": 2})
    elif change == "budget_roots":
        args["policy"] = args["policy"].model_copy(update={"max_services": 1})
    elif change == "corrupt_snapshot":
        args["snapshot"] = args["snapshot"].model_copy(
            update={"snapshot_fingerprint": "a" * 64}
        )
    elif change == "copied_bool_revision":
        args["incident"] = args["incident"].model_copy(update={"revision": True})
    elif change == "copied_extra_incident":
        args["incident"] = args["incident"].model_copy(
            update={"root_cause": "forbidden"}
        )
    with pytest.raises(expected) as caught:
        calculate_blast_radius(**args)
    assert type(caught.value) is expected
    assert len(str(caught.value)) < 256


def _synthetic_inputs(
    count, edges, roots=(0,), hops=2, direction="downstream", **overrides
):
    from app.correlation.models import TopologySnapshotReference
    from app.correlation.topology import (
        TopologyVersion,
        TopologyServiceNode,
        DirectedTopologyEdge,
    )
    from app.simulator.interfaces import ServiceType, DependencyType
    from datetime import datetime, timezone

    snapshot = TopologySnapshot.create(
        version=TopologyVersion(
            topology_id=UUID(int=100), topology_version="boundary/1"
        ),
        nodes=tuple(
            TopologyServiceNode(
                service_id=UUID(int=i + 1), name=f"s{i}", service_type=ServiceType.API
            )
            for i in range(count)
        ),
        edges=tuple(
            DirectedTopologyEdge(
                upstream_service_id=UUID(int=a + 1),
                downstream_service_id=UUID(int=b + 1),
                dependency_type=DependencyType(t),
            )
            for a, b, t in edges
        ),
    )
    return dict(
        incident=_incident_for_services([f"s{i}" for i in roots]),
        snapshot=snapshot,
        expected_topology=TopologySnapshotReference(
            topology_id=snapshot.version.topology_id,
            topology_version=snapshot.version.topology_version,
            snapshot_fingerprint=snapshot.snapshot_fingerprint,
            captured_at=datetime(2026, 11, 21, tzinfo=timezone.utc),
            provider="canonical",
        ),
        policy=BlastRadiusPolicy.model_validate_json(
            encode(dict(max_hops=hops, direction=direction, **overrides))
        ),
        as_of=datetime(2026, 11, 21, 0, 1, tzinfo=timezone.utc),
    )


def test_maximum_hops_still_reports_actual_omission_and_boundary_work():
    from app.correlation.blast_radius import calculate_blast_radius

    args = _synthetic_inputs(34, [(i, i + 1, "sync") for i in range(33)], hops=32)
    result = calculate_blast_radius(**args)
    assert len(result.paths) == 33 and result.paths[-1].hop_count == 32
    assert result.truncation_reasons == ("hop_limit",) and result.examined_arcs == 33


def test_full_snapshot_ceiling_is_bounded_and_service_truncation_is_measured():
    from app.correlation.blast_radius import calculate_blast_radius

    args = _synthetic_inputs(4096, [(0, i, "sync") for i in range(1, 4096)], hops=1)
    result = calculate_blast_radius(**args)
    assert len(result.paths) == len(result.reachable_services) == 512
    assert len(result.edges) == 511 and result.omitted_within_hop_services == 3584
    assert result.omitted_eligible_edges == 3584 and result.examined_arcs == 4095
    assert result.truncation_reasons == ("service_limit",)


def test_all_512_roots_are_retained_and_513_are_rejected():
    from app.correlation.blast_radius import calculate_blast_radius

    args = _synthetic_inputs(513, [], roots=tuple(range(512)), hops=0, max_edges=0)
    result = calculate_blast_radius(**args)
    assert len(result.member_services) == len(result.paths) == 512
    assert not result.truncated and result.examined_arcs == 0
    args["incident"] = _incident_for_services([f"s{i}" for i in range(513)])
    with pytest.raises(BlastRadiusInputError):
        calculate_blast_radius(**args)


def test_exact_edge_output_ceiling_keeps_witnesses_before_other_edges():
    from app.correlation.blast_radius import calculate_blast_radius

    edges = [
        (a, b, t)
        for a in range(32)
        for b in range(32)
        if a != b
        for t in ("data", "sync")
    ]
    result = calculate_blast_radius(
        **_synthetic_inputs(32, edges, hops=1, direction="both")
    )
    assert len(result.edges) == 1024 and len(result.paths) == 32
    assert result.examined_arcs == 3968 and result.omitted_eligible_edges == 960
    assert result.truncation_reasons == ("edge_limit",)
    retained = {
        (e.upstream_service_id, e.downstream_service_id, e.dependency_type)
        for e in result.edges
    }
    assert all(
        (e.upstream_service_id, e.downstream_service_id, e.dependency_type) in retained
        for p in result.paths
        for e in p.edges
    )


@pytest.mark.parametrize("which", ["nodes", "edges"])
def test_unsafe_snapshot_count_overflow_rejected_before_revalidation(
    monkeypatch, which
):
    import app.correlation.blast_radius as module

    args = _calculator_inputs()
    size = 4097 if which == "nodes" else 16385
    args["snapshot"] = args["snapshot"].model_copy(
        update={which: (getattr(args["snapshot"], which)[0],) * size}
    )

    def no_validation(*a, **kw):
        raise AssertionError("Revalidation started before count guard")

    monkeypatch.setattr(module, "_fresh", no_validation)
    with pytest.raises(BlastRadiusLimitError):
        module.calculate_blast_radius(**args)


def test_dense_valid_snapshot_at_edge_ceiling_does_not_enumerate_paths():
    from app.correlation.blast_radius import calculate_blast_radius

    edges = [
        (a, b, t)
        for a in range(130)
        for b in range(130)
        if a != b
        for t in ("data", "sync")
    ][:16384]
    result = calculate_blast_radius(**_synthetic_inputs(130, edges, hops=0))
    assert len(result.paths) == 1 and result.examined_arcs == 258
    assert result.truncation_reasons == ("hop_limit",)


def test_context_byte_ceiling_rejects_large_but_count_bounded_witness_output():
    from app.correlation.blast_radius import calculate_blast_radius

    edges = [(i, i + 1, "sync") for i in range(31)] + [
        (31, i, "sync") for i in range(32, 512)
    ]
    with pytest.raises(BlastRadiusLimitError):
        calculate_blast_radius(**_synthetic_inputs(512, edges, hops=32))


def test_age_equality_and_matching_incident_pin_are_accepted_without_mutation():
    from app.correlation.blast_radius import calculate_blast_radius
    from app.correlation.models import IncidentAggregate
    from app.correlation.serialization import serialize_incident_aggregate

    args = _calculator_inputs()
    args["policy"] = args["policy"].model_copy(update={"max_snapshot_age_seconds": 60})
    fields = args["incident"].model_dump(
        mode="python", exclude={"revision_fingerprint"}
    )
    fields["topology_snapshot"] = args["expected_topology"]
    args["incident"] = IncidentAggregate.create(**fields)
    before = serialize_incident_aggregate(args["incident"])
    result = calculate_blast_radius(**args)
    assert result.topology_snapshot == args["expected_topology"]
    assert serialize_incident_aggregate(args["incident"]) == before


def test_duplicate_member_service_keeps_both_signals_but_only_one_root():
    from app.correlation.blast_radius import calculate_blast_radius
    from app.correlation.serialization import serialize_incident_aggregate
    from test_correlation_handoff import incident_fixture

    args = _calculator_inputs()
    args["incident"] = incident_fixture()  # two distinct signals for order-service
    before = serialize_incident_aggregate(args["incident"])
    result = calculate_blast_radius(**args)
    assert len(result.member_services) == 1
    assert len(args["incident"].signal_references) == 2
    assert serialize_incident_aggregate(args["incident"]) == before


def test_legacy_affected_services_do_not_become_additional_roots():
    from app.correlation.blast_radius import calculate_blast_radius
    from app.correlation.models import IncidentAggregate

    args = _calculator_inputs()
    fields = args["incident"].model_dump(
        mode="python", exclude={"revision_fingerprint"}
    )
    fields["affected_services"] = ("client", "order-service")
    args["incident"] = IncidentAggregate.create(**fields)
    result = calculate_blast_radius(**args)
    assert [n.name for n in result.member_services] == ["order-service"]
    assert "client" not in {n.name for n in result.reachable_services}


def test_changed_snapshot_content_under_same_version_is_not_silently_re_pinned():
    from app.correlation.blast_radius import (
        calculate_blast_radius,
        BlastRadiusTopologyMismatchError,
    )
    from app.correlation.topology import TopologyServiceNode
    from app.simulator.interfaces import ServiceType

    args = _calculator_inputs()
    previous = args["snapshot"]
    extra = TopologyServiceNode(
        service_id=UUID(int=987),
        name="disconnected-extra",
        service_type=ServiceType.API,
    )
    args["snapshot"] = TopologySnapshot.create(
        version=previous.version, nodes=(*previous.nodes, extra), edges=previous.edges
    )
    assert args["snapshot"].version == previous.version
    assert (
        args["snapshot"].snapshot_fingerprint
        != args["expected_topology"].snapshot_fingerprint
    )
    with pytest.raises(BlastRadiusTopologyMismatchError):
        calculate_blast_radius(**args)


def test_service_names_are_case_sensitive_without_fuzzy_fallback():
    from app.correlation.blast_radius import calculate_blast_radius

    args = _calculator_inputs()
    args["incident"] = _incident_for_services(["Order-service"])
    with pytest.raises(BlastRadiusInputError):
        calculate_blast_radius(**args)


def test_invalid_raw_incident_state_is_rejected_before_legacy_serialization(
    monkeypatch,
):
    import app.correlation.blast_radius as module

    args = _calculator_inputs()
    args["incident"] = args["incident"].model_copy(update={"revision": True})
    calls = []

    def forbidden(*a, **kw):
        calls.append(True)
        raise AssertionError("Legacy serializer saw unvalidated source")

    monkeypatch.setattr(module, "serialize_incident_aggregate", forbidden)
    with pytest.raises(BlastRadiusInputError):
        module.calculate_blast_radius(**args)
    assert calls == []
