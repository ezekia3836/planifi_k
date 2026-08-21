-- =====================================================================
-- 1. Table reporting : grain événement (inchangée, léger, pas de merge PG)
-- =====================================================================
CREATE TABLE IF NOT EXISTS reporting
(
    database_id  Int32,
    ktk_id       Int32,
    dwh_id       String,
    country      Int32,
    segmentId    Int32,
    subject      String,
    brand        String,
    tag_id       Int32,
    adv_id       Int32,
    id_routers   Int64,
    ListId       Int32,
    ListName     String,
    zipcode      String,
    dep          String,
    sends        UInt8,
    opens        UInt8,
    clicks       UInt8,
    unsubs       UInt8,
    complaints   UInt8,
    bounces      UInt8,
    age_range    String,
    gender       String,
    main_isp     String,
    date_event   Date,
    updated_at   DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date_event)
ORDER BY (date_event, id_routers, dwh_id, tag_id);


-- =====================================================================
-- 2. Table reporting_focus : grain id_routers, alimentée par PG agrégé
--    Une ligne par id_routers, ReplacingMergeTree pour dédupliquer
--    proprement à chaque réinsertion (mise à jour = nouvelle ligne
--    avec updated_at plus récent).
-- =====================================================================
CREATE TABLE IF NOT EXISTS reporting_focus
(
    id_routers   Int64,
    id_focus     Array(Int64),
    ca           Float64,
    clicks_val   Float64,
    leads_val    Float64,
    sales_val    Float64,
    cpm_val      Float64,
    model        String,      -- JSON string : liste de {model, payvalue, comment}
    updated_at   DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id_routers;


-- =====================================================================
-- 3. Dictionary : charge reporting_focus en mémoire (hashé),
--    rafraîchi automatiquement toutes les 5-10 min, lookup O(1).
--    Table PETITE (une ligne par id_routers) donc RAM faible.
-- =====================================================================
CREATE DICTIONARY IF NOT EXISTS reporting_focus_dict
(
    id_routers   Int64,
    id_focus     Array(Int64),
    ca           Float64,
    clicks_val   Float64,
    leads_val    Float64,
    sales_val    Float64,
    cpm_val      Float64,
    model        String
)
PRIMARY KEY id_routers
SOURCE(CLICKHOUSE(
    TABLE 'reporting_focus'
    -- adapter host/user/password/db si le dictionary tourne sur un autre noeud
))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED());


-- =====================================================================
-- 4. Vue enrichie : ce que FastAPI interroge. Zéro JOIN, zéro
--    duplication physique — le dictGet fait un lookup en mémoire.
-- =====================================================================
CREATE VIEW IF NOT EXISTS reporting_enriched AS
SELECT
    r.*,
    dictGetOrDefault('reporting_focus_dict', 'ca',         r.id_routers, 0.0)  AS ca,
    dictGetOrDefault('reporting_focus_dict', 'clicks_val', r.id_routers, 0.0)  AS clicks_val,
    dictGetOrDefault('reporting_focus_dict', 'leads_val',  r.id_routers, 0.0)  AS leads_val,
    dictGetOrDefault('reporting_focus_dict', 'sales_val',  r.id_routers, 0.0)  AS sales_val,
    dictGetOrDefault('reporting_focus_dict', 'cpm_val',    r.id_routers, 0.0)  AS cpm_val,
    dictGetOrDefault('reporting_focus_dict', 'model',      r.id_routers, '[]') AS model,
    dictGetOrDefault('reporting_focus_dict', 'id_focus',   r.id_routers, [])   AS id_focus
FROM reporting r;
