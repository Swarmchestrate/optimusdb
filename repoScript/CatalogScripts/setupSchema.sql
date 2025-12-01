-- ============================================================================
-- SAMPLE DATA FOR TESTING AMUNDSEN INTEGRATION
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sample Users
-- ----------------------------------------------------------------------------
INSERT OR REPLACE INTO users VALUES
    ('user_001', 'test_user_id', 'test@email.com', 'Test User', 'Test', 'User',
     '', '', '', 'Data Science', 'Engineering', 'Data Engineer', 'full-time',
     NULL, NULL, 1, strftime('%s', 'now'), strftime('%s', 'now'), NULL),

    ('user_002', 'john_doe', 'john.doe@company.com', 'John Doe', 'John', 'Doe',
     'http://company.com/profiles/john', 'johndoe', 'U123456', 'ML Platform',
     'Engineering', 'ML Engineer', 'full-time', NULL, NULL, 1,
     strftime('%s', 'now'), strftime('%s', 'now'), NULL),

    ('user_003', 'ml_team', 'ml-team@company.com', 'ML Team', 'ML', 'Team',
     '', '', 'U789012', 'ML Platform', 'Engineering', 'Team Account', 'team',
     NULL, NULL, 1, strftime('%s', 'now'), strftime('%s', 'now'), NULL);

-- ----------------------------------------------------------------------------
-- Sample Dashboards
-- ----------------------------------------------------------------------------
INSERT OR REPLACE INTO dashboards VALUES
    ('dash_001', 'recommendation_performance',
     'Recommendation Engine Performance',
     'http://dashboards.company.com/recommendation_performance',
     'Real-time monitoring of recommendation engine metrics including CTR, conversion rate, and model accuracy',
     'Data Science', 'http://dashboards.company.com/datascience',
     'ML Platform', 'default', 'ml_team', 'ml_team,john_doe',
     'ML,Recommendations,Performance', 'Production,Real-time',
     0, NULL, strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'),
     'operational', '5 minutes', '["Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores"]'),

    ('dash_002', 'user_engagement_analytics',
     'User Engagement Analytics',
     'http://dashboards.company.com/user_engagement',
     'Track user behavior patterns, session duration, and feature adoption',
     'Product Analytics', 'http://dashboards.company.com/product',
     'Analytics Platform', 'default', 'john_doe', 'john_doe',
     'Users,Engagement,Analytics', 'Production',
     0, NULL, strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'),
     'analytical', '1 hour', '["Data Science://optimusdb.User Behavior/User Click Events"]'),

    ('dash_003', 'data_quality_dashboard',
     'Data Quality Monitoring',
     'http://dashboards.company.com/data_quality',
     'Monitor data quality metrics, freshness, and completeness across all datasets',
     'Data Engineering', 'http://dashboards.company.com/dataeng',
     'Data Platform', 'default', 'test_user_id', 'test_user_id,ml_team',
     'Quality,Monitoring,Operations', 'Production,Verified',
     0, NULL, strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'),
     'operational', '15 minutes', NULL);

-- ----------------------------------------------------------------------------
-- Sample Tables in datacatalog
-- ----------------------------------------------------------------------------

-- Table 1: Recommendation Engine - Product Recommendation Scores
INSERT OR REPLACE INTO datacatalog VALUES
    ('rec_engine_001',
     'Recommendation Engine.Product Recommendation Scores',
     'Recommendation Engine',
     'Real-time product recommendation scores using collaborative filtering and content-based algorithms. Powers personalized product suggestions across web, mobile, and email channels.',
     'Data Science',
     'ml_team',
     'ML Team',
     'ML,Real-time,Recommendations,Production',
     'Production,Verified,ML Model',
     'high',
     'active',
     'production',
     'v2.3.1',
     -- Columns JSON
     '[
         {"name":"user_id","type":"varchar","description":"Unique user identifier","sample_value":"USR_12345","is_nullable":false,"is_primary_key":true},
         {"name":"product_id","type":"varchar","description":"Product identifier","sample_value":"PROD_67890","is_nullable":false,"is_primary_key":true},
         {"name":"score","type":"float","description":"Recommendation confidence score (0.0-1.0)","sample_value":"0.87","is_nullable":false},
         {"name":"model_version","type":"varchar","description":"ML model version used","sample_value":"v2.3.1","is_nullable":false},
         {"name":"features_used","type":"varchar","description":"Comma-separated feature names","sample_value":"user_history,product_category,collaborative_filter","is_nullable":true},
         {"name":"timestamp","type":"timestamp","description":"Time when recommendation was generated","sample_value":"2024-12-01T19:57:00Z","is_nullable":false},
         {"name":"session_id","type":"varchar","description":"User session identifier","sample_value":"SESS_ABC123","is_nullable":true}
     ]',
     -- Lineage upstream
     '[
         {"key":"Data Science://optimusdb.User Behavior/User Click Events","level":1},
         {"key":"Data Science://optimusdb.Product Catalog/Product Attributes","level":1},
         {"key":"Data Science://optimusdb.User Profiles/User Preferences","level":1}
     ]',
     -- Lineage downstream
     '[
         {"key":"Data Science://optimusdb.Analytics/Recommendation Performance Metrics","level":1},
         {"key":"dashboard://recommendation_performance","level":1}
     ]',
     'ml_team,john_doe',
     '{"primary":"ml_team","contributors":["john_doe"],"approvers":["ml_team"]}',
     0.95,
     strftime('%s', 'now'),
     'Internal',
     1250000,
     strftime('%s', 'now'),
     125000000,
     1024000000,
     '{"row_count":125000000,"size_mb":976,"avg_score":0.72,"distinct_users":5000000}',
     'fact',
     'real-time',
     'Continuous streaming',
     's3://ml-data/recommendations/',
     'date',
     'user_id,product_id',
     'https://wiki.company.com/ml/recommendations',
     'https://wiki.company.com/ml/recommendations',
     '#ml-platform',
     '["Data Science://optimusdb.User Behavior/User Purchase History"]',
     NULL,
     NULL,
     '{"type":"streaming","latency":"<100ms"}',
     '{"latency":"<100ms","availability":"99.9%"}',
     'High-throughput real-time scoring',
     'CREATE TABLE recommendations AS SELECT * FROM ml_model_output;',
     'Collaborative filtering + Content-based hybrid model',
     strftime('%s', 'now') - 7776000,  -- 90 days ago
     strftime('%s', 'now'),
     'recommendation,product,score,user,ml');

-- Table 2: User Behavior - User Click Events
INSERT OR REPLACE INTO datacatalog VALUES
    ('user_clicks_001',
     'User Click Events',
     'User Behavior',
     'Raw clickstream data capturing all user interactions across web and mobile platforms. Includes page views, button clicks, and navigation events.',
     'Data Science',
     'john_doe',
     'John Doe',
     'Events,User Behavior,Clickstream,Raw',
     'Production,Real-time',
     'medium',
     'active',
     'production',
     'v1.0.0',
     '[
         {"name":"event_id","type":"varchar","description":"Unique event identifier","sample_value":"EVT_789012","is_nullable":false,"is_primary_key":true},
         {"name":"user_id","type":"varchar","description":"User identifier","sample_value":"USR_12345","is_nullable":false},
         {"name":"session_id","type":"varchar","description":"Session identifier","sample_value":"SESS_ABC123","is_nullable":false},
         {"name":"event_type","type":"varchar","description":"Type of event (click, view, scroll)","sample_value":"click","is_nullable":false},
         {"name":"target_element","type":"varchar","description":"HTML element identifier","sample_value":"btn_add_to_cart","is_nullable":true},
         {"name":"page_url","type":"varchar","description":"Full page URL","sample_value":"https://shop.com/products/123","is_nullable":false},
         {"name":"timestamp","type":"timestamp","description":"Event timestamp","sample_value":"2024-12-01T19:57:00Z","is_nullable":false},
         {"name":"device_type","type":"varchar","description":"Device type (desktop, mobile, tablet)","sample_value":"mobile","is_nullable":true},
         {"name":"platform","type":"varchar","description":"Platform (web, ios, android)","sample_value":"ios","is_nullable":true}
     ]',
     '[]',
     '[
         {"key":"Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores","level":1},
         {"key":"Data Science://optimusdb.Analytics/User Engagement Metrics","level":1}
     ]',
     'john_doe',
     '{"primary":"john_doe","contributors":["test_user_id"]}',
     0.88,
     strftime('%s', 'now'),
     'PII',
     5500000,
     strftime('%s', 'now'),
     500000000,
     2048000000,
     '{"row_count":500000000,"size_mb":1953,"events_per_day":5000000}',
     'fact',
     'real-time',
     'Continuous streaming',
     's3://events/clickstream/',
     'date',
     'user_id,timestamp',
     'https://wiki.company.com/analytics/clickstream',
     'https://wiki.company.com/analytics/clickstream',
     '#data-platform',
     NULL,
     NULL,
     NULL,
     '{"type":"streaming","latency":"<1s"}',
     '{"latency":"<5s","availability":"99.95%"}',
     'High-volume event stream',
     NULL,
     'Direct stream from application events',
     strftime('%s', 'now') - 15552000,  -- 180 days ago
     strftime('%s', 'now'),
     'clicks,events,behavior,users');

-- Table 3: Product Catalog - Product Attributes
INSERT OR REPLACE INTO datacatalog VALUES
    ('product_cat_001',
     'Product Attributes',
     'Product Catalog',
     'Master product catalog with detailed attributes, categories, pricing, and inventory information. Updated daily from product management systems.',
     'Data Science',
     'test_user_id',
     'Test User',
     'Products,Catalog,Reference,Master Data',
     'Production,Verified',
     'high',
     'active',
     'production',
     'v3.1.0',
     '[
         {"name":"product_id","type":"varchar","description":"Unique product identifier","sample_value":"PROD_67890","is_nullable":false,"is_primary_key":true},
         {"name":"product_name","type":"varchar","description":"Product display name","sample_value":"Wireless Headphones","is_nullable":false},
         {"name":"category","type":"varchar","description":"Product category","sample_value":"Electronics > Audio","is_nullable":false},
         {"name":"subcategory","type":"varchar","description":"Product subcategory","sample_value":"Headphones","is_nullable":true},
         {"name":"brand","type":"varchar","description":"Brand name","sample_value":"TechBrand","is_nullable":false},
         {"name":"price","type":"float","description":"Current price in USD","sample_value":"79.99","is_nullable":false},
         {"name":"inventory_count","type":"int","description":"Available inventory","sample_value":"150","is_nullable":false},
         {"name":"rating","type":"float","description":"Average customer rating (0-5)","sample_value":"4.5","is_nullable":true},
         {"name":"review_count","type":"int","description":"Total number of reviews","sample_value":"234","is_nullable":true},
         {"name":"is_active","type":"boolean","description":"Product availability status","sample_value":"true","is_nullable":false},
         {"name":"created_date","type":"timestamp","description":"Product creation date","sample_value":"2023-06-15T10:00:00Z","is_nullable":false},
         {"name":"updated_date","type":"timestamp","description":"Last update timestamp","sample_value":"2024-12-01T08:00:00Z","is_nullable":false}
     ]',
     '[]',
     '[
         {"key":"Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores","level":1}
     ]',
     'test_user_id,ml_team',
     '{"primary":"test_user_id","contributors":["ml_team"]}',
     0.98,
     strftime('%s', 'now'),
     NULL,
     150,
     strftime('%s', 'now'),
     45000,
     10240000,
     '{"row_count":45000,"size_mb":9,"categories":150,"brands":500}',
     'dimension',
     'daily',
     '0 2 * * *',
     's3://master-data/products/',
     NULL,
     'product_id',
     'https://wiki.company.com/catalog/products',
     'https://wiki.company.com/catalog/products',
     '#product-team',
     NULL,
     NULL,
     NULL,
     '{"type":"batch","schedule":"daily 2am UTC"}',
     '{"freshness":"<24h","completeness":"99%"}',
     'Reference data for product information',
     NULL,
     'Synchronized from ProductDB',
     strftime('%s', 'now') - 31536000,  -- 365 days ago
     strftime('%s', 'now'),
     'products,catalog,reference,master');

-- Table 4: Analytics - Recommendation Performance Metrics
INSERT OR REPLACE INTO datacatalog VALUES
    ('rec_perf_001',
     'Recommendation Performance Metrics',
     'Analytics',
     'Aggregated metrics tracking recommendation engine performance including CTR, conversion rates, revenue impact, and model accuracy. Updated hourly.',
     'Data Science',
     'ml_team',
     'ML Team',
     'Analytics,ML,Performance,Metrics',
     'Production,ML Model',
     'high',
     'active',
     'production',
     'v1.2.0',
     '[
         {"name":"metric_date","type":"date","description":"Date of metrics","sample_value":"2024-12-01","is_nullable":false,"is_primary_key":true},
         {"name":"metric_hour","type":"int","description":"Hour of day (0-23)","sample_value":"15","is_nullable":false,"is_primary_key":true},
         {"name":"model_version","type":"varchar","description":"Model version","sample_value":"v2.3.1","is_nullable":false},
         {"name":"recommendations_served","type":"int","description":"Total recommendations shown","sample_value":"1250000","is_nullable":false},
         {"name":"clicks","type":"int","description":"Total clicks on recommendations","sample_value":"187500","is_nullable":false},
         {"name":"conversions","type":"int","description":"Purchases from recommendations","sample_value":"15000","is_nullable":false},
         {"name":"ctr","type":"float","description":"Click-through rate","sample_value":"0.15","is_nullable":false},
         {"name":"conversion_rate","type":"float","description":"Conversion rate","sample_value":"0.08","is_nullable":false},
         {"name":"revenue_usd","type":"float","description":"Revenue generated (USD)","sample_value":"1200000.50","is_nullable":false},
         {"name":"avg_score","type":"float","description":"Average recommendation score","sample_value":"0.72","is_nullable":false}
     ]',
     '[
         {"key":"Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores","level":1},
         {"key":"Data Science://optimusdb.User Behavior/User Click Events","level":1}
     ]',
     '[
         {"key":"dashboard://recommendation_performance","level":1}
     ]',
     'ml_team',
     '{"primary":"ml_team"}',
     0.99,
     strftime('%s', 'now'),
     NULL,
     250,
     strftime('%s', 'now'),
     8760,
     5120000,
     '{"row_count":8760,"size_mb":4,"avg_ctr":0.15}',
     'aggregate',
     'hourly',
     '0 * * * *',
     's3://analytics/recommendation-metrics/',
     'metric_date',
     'metric_date,metric_hour',
     'https://wiki.company.com/ml/metrics',
     'https://wiki.company.com/ml/metrics',
     '#ml-platform',
     NULL,
     NULL,
     NULL,
     '{"type":"batch","schedule":"hourly"}',
     '{"latency":"<1h","completeness":"100%"}',
     'Hourly aggregated performance metrics',
     'CREATE TABLE metrics AS SELECT date, hour, COUNT(*), AVG(score) FROM recommendations GROUP BY date, hour;',
     'Aggregation of recommendation events',
     strftime('%s', 'now') - 7776000,  -- 90 days ago
     strftime('%s', 'now'),
     'metrics,performance,analytics,ml');

-- ----------------------------------------------------------------------------
-- Table-Dashboard Relations
-- ----------------------------------------------------------------------------
INSERT OR REPLACE INTO table_dashboard_relations VALUES
    ('tdr_001', 'Data Science://optimusdb.Recommendation_Engine/Recommendation_Engine.Product_Recommendation_Scores', 'recommendation_performance', strftime('%s', 'now')),
    ('tdr_002', 'Data Science://optimusdb.Analytics/Recommendation_Performance_Metrics', 'recommendation_performance', strftime('%s', 'now')),
    ('tdr_003', 'Data Science://optimusdb.User_Behavior/User_Click_Events', 'user_engagement_analytics', strftime('%s', 'now'));

-- ----------------------------------------------------------------------------
-- User-Resource Relations (Follows/Bookmarks)
-- ----------------------------------------------------------------------------
INSERT OR REPLACE INTO user_resource_relations VALUES
    ('urr_001', 'rec_engine_001', 'test_user_id', 'follow', 'table', strftime('%s', 'now')),
    ('urr_002', 'rec_engine_001', 'john_doe', 'follow', 'table', strftime('%s', 'now')),
    ('urr_003', 'user_clicks_001', 'test_user_id', 'follow', 'table', strftime('%s', 'now')),
    ('urr_004', 'recommendation_performance', 'test_user_id', 'follow', 'dashboard', strftime('%s', 'now')),
    ('urr_005', 'recommendation_performance', 'ml_team', 'own', 'dashboard', strftime('%s', 'now'));

-- ----------------------------------------------------------------------------
-- Sample Access Logs
-- ----------------------------------------------------------------------------
INSERT OR REPLACE INTO access_log VALUES
    ('log_001', 'rec_engine_001', 'table', 'test_user_id', 'view', strftime('%s', 'now'), 'web', 1200, 1),
    ('log_002', 'rec_engine_001', 'table', 'john_doe', 'query', strftime('%s', 'now') - 3600, 'api', 3500, 1),
    ('log_003', 'user_clicks_001', 'table', 'test_user_id', 'view', strftime('%s', 'now') - 7200, 'web', 800, 1),
    ('log_004', 'recommendation_performance', 'dashboard', 'ml_team', 'view', strftime('%s', 'now') - 1800, 'web', 2500, 1);

-- ============================================================================
-- END OF SAMPLE DATA
-- ============================================================================