-- Bronze: calendar (date dimension) and weekly sell prices.

DROP TABLE IF EXISTS bronze.calendar;
CREATE TABLE bronze.calendar (
    date           DATE     NOT NULL,
    wm_yr_wk       INTEGER  NOT NULL,        -- Walmart fiscal year-week id
    weekday        TEXT     NOT NULL,
    wday           INTEGER  NOT NULL,        -- 1..7
    month          INTEGER  NOT NULL,
    year           INTEGER  NOT NULL,
    d              TEXT     NOT NULL,        -- d_1, d_2, ... the JOIN KEY to sales
    event_name_1   TEXT,                     -- nullable: most days have no event
    event_type_1   TEXT,
    event_name_2   TEXT,
    event_type_2   TEXT,
    snap_CA        INTEGER  NOT NULL,        -- 0/1 flags
    snap_TX        INTEGER  NOT NULL,
    snap_WI        INTEGER  NOT NULL
);

DROP TABLE IF EXISTS bronze.sell_prices;
CREATE TABLE bronze.sell_prices (
    store_id   TEXT     NOT NULL,
    item_id    TEXT     NOT NULL,
    wm_yr_wk   INTEGER  NOT NULL,            -- joins to calendar.wm_yr_wk
    sell_price NUMERIC(10,4) NOT NULL        -- precise enough for prices
);