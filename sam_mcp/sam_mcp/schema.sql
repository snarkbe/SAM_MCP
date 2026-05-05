PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS atc (
    code        TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS substance (
    code    TEXT PRIMARY KEY,
    name_fr TEXT,
    name_nl TEXT,
    name_en TEXT,
    type    TEXT
);

CREATE TABLE IF NOT EXISTS pharma_form (
    code    TEXT PRIMARY KEY,
    name_fr TEXT,
    name_nl TEXT,
    name_en TEXT
);

CREATE TABLE IF NOT EXISTS route (
    code    TEXT PRIMARY KEY,
    name_fr TEXT,
    name_nl TEXT,
    name_en TEXT
);

CREATE TABLE IF NOT EXISTS amp (
    code                 TEXT PRIMARY KEY,
    name_fr              TEXT,
    name_nl              TEXT,
    name_en              TEXT,
    official_name        TEXT,
    status               TEXT,
    medicine_type        TEXT,
    black_triangle       INTEGER,
    company              TEXT,
    prescription_name_fr TEXT,
    prescription_name_nl TEXT,
    valid_from           TEXT,
    valid_to             TEXT
);

CREATE TABLE IF NOT EXISTS amp_component (
    amp_code         TEXT,
    seq              INTEGER,
    pharma_form_code TEXT,
    pharma_form_fr   TEXT,
    pharma_form_nl   TEXT,
    route_code       TEXT,
    route_fr         TEXT,
    route_nl         TEXT,
    PRIMARY KEY (amp_code, seq)
);

CREATE TABLE IF NOT EXISTS amp_ingredient (
    amp_code           TEXT,
    component_seq      INTEGER,
    rank               INTEGER,
    type               TEXT,
    substance_code     TEXT,
    substance_name_fr  TEXT,
    substance_name_nl  TEXT,
    substance_name_en  TEXT,
    strength_operator  TEXT,
    strength_quantity  TEXT,
    strength_unit      TEXT
);

CREATE TABLE IF NOT EXISTS ampp (
    cti_extended         TEXT PRIMARY KEY,
    amp_code             TEXT,
    auth_nr              TEXT,
    pack_display_fr      TEXT,
    pack_display_nl      TEXT,
    status               TEXT,
    prescription_name_fr TEXT,
    prescription_name_nl TEXT,
    delivery_modus       TEXT,
    legal_basis_fr       TEXT,
    ex_factory_price     REAL
);

CREATE TABLE IF NOT EXISTS dmpp (
    cnk                  TEXT PRIMARY KEY,
    cti_extended         TEXT,
    amp_code             TEXT,
    delivery_environment TEXT,
    product_id           TEXT
);

CREATE INDEX IF NOT EXISTS idx_ing_amp        ON amp_ingredient(amp_code);
CREATE INDEX IF NOT EXISTS idx_ing_substance  ON amp_ingredient(substance_code);
CREATE INDEX IF NOT EXISTS idx_ampp_amp       ON ampp(amp_code);
CREATE INDEX IF NOT EXISTS idx_dmpp_amp       ON dmpp(amp_code);
CREATE INDEX IF NOT EXISTS idx_dmpp_cti       ON dmpp(cti_extended);
CREATE INDEX IF NOT EXISTS idx_amp_comp       ON amp_component(amp_code);

CREATE VIRTUAL TABLE IF NOT EXISTS amp_fts USING fts5(
    amp_code UNINDEXED,
    name_fr, name_nl, name_en, official_name,
    prescription_name_fr, prescription_name_nl,
    tokenize='unicode61 remove_diacritics 2'
);

CREATE VIRTUAL TABLE IF NOT EXISTS substance_fts USING fts5(
    substance_code UNINDEXED,
    name_fr, name_nl, name_en,
    tokenize='unicode61 remove_diacritics 2'
);
