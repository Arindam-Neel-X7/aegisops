CREATE CONSTRAINT service_id_unique IF NOT EXISTS
FOR (s:Service)
REQUIRE s.service_id IS UNIQUE;

MATCH (s:Service)
WHERE s.service_id = '29fb0a3b-31a1-527b-9ecf-b50cba780338'
SET s.name = 'client',
    s.service_type = 'external',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = 'a423c462-9e3d-5df7-9b88-2a71637a958f'
SET s.name = 'api-gateway',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = 'fdface04-c1df-5daa-ab47-23debcebb2fd'
SET s.name = 'order-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = '2b8e9c49-61b0-5a53-821d-6193136b1db2'
SET s.name = 'payment-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = '936fb446-a6f1-5cf9-88d2-65789b6e6eae'
SET s.name = 'inventory-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = '10ed11a8-4faf-50f5-9903-2417c842278c'
SET s.name = 'notification-service',
    s.service_type = 'worker',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (s:Service)
WHERE s.service_id = 'a50eab12-f3a8-544a-be74-060c56b3e254'
SET s.name = 'database',
    s.service_type = 'database',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: '29fb0a3b-31a1-527b-9ecf-b50cba780338'})
SET s.name = 'client',
    s.service_type = 'external',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: 'a423c462-9e3d-5df7-9b88-2a71637a958f'})
SET s.name = 'api-gateway',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: 'fdface04-c1df-5daa-ab47-23debcebb2fd'})
SET s.name = 'order-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: '2b8e9c49-61b0-5a53-821d-6193136b1db2'})
SET s.name = 'payment-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: '936fb446-a6f1-5cf9-88d2-65789b6e6eae'})
SET s.name = 'inventory-service',
    s.service_type = 'api',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: '10ed11a8-4faf-50f5-9903-2417c842278c'})
SET s.name = 'notification-service',
    s.service_type = 'worker',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MERGE (s:Service {service_id: 'a50eab12-f3a8-544a-be74-060c56b3e254'})
SET s.name = 'database',
    s.service_type = 'database',
    s.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    s.topology_version = '1.0.0';

MATCH (u:Service {service_id: '29fb0a3b-31a1-527b-9ecf-b50cba780338'}), (d:Service {service_id: 'a423c462-9e3d-5df7-9b88-2a71637a958f'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'sync',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: 'a423c462-9e3d-5df7-9b88-2a71637a958f'}), (d:Service {service_id: 'fdface04-c1df-5daa-ab47-23debcebb2fd'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'sync',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: 'fdface04-c1df-5daa-ab47-23debcebb2fd'}), (d:Service {service_id: '2b8e9c49-61b0-5a53-821d-6193136b1db2'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'sync',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: '2b8e9c49-61b0-5a53-821d-6193136b1db2'}), (d:Service {service_id: 'a50eab12-f3a8-544a-be74-060c56b3e254'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'data',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: 'fdface04-c1df-5daa-ab47-23debcebb2fd'}), (d:Service {service_id: '936fb446-a6f1-5cf9-88d2-65789b6e6eae'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'sync',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: '936fb446-a6f1-5cf9-88d2-65789b6e6eae'}), (d:Service {service_id: 'a50eab12-f3a8-544a-be74-060c56b3e254'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'data',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';

MATCH (u:Service {service_id: 'fdface04-c1df-5daa-ab47-23debcebb2fd'}), (d:Service {service_id: '10ed11a8-4faf-50f5-9903-2417c842278c'})
MERGE (u)-[r:DEPENDS_ON]->(d)
SET r.dependency_type = 'async',
    r.topology_id = '77bb041d-fb4c-5370-9b99-45bef0082479',
    r.topology_version = '1.0.0';
