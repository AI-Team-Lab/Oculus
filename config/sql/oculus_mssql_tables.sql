--+------------------+--
--| Data Lake Tables |--
--+------------------+--

CREATE TABLE dl.willhaben
(
    id                    BIGINT NOT NULL PRIMARY KEY,
    advertStatus          NVARCHAR(50),
    make                  NVARCHAR(100),
    model                 NVARCHAR(100),
    specification         NVARCHAR(MAX),
    description_head      NVARCHAR(MAX),
    description           NVARCHAR(MAX),
    year_model            INT,
    transmission          NVARCHAR(50),
    transmission_resolved NVARCHAR(50),
    mileage               INT,
    noofseats             INT,
    engine_effect         INT,
    engine_fuel           NVARCHAR(50),
    engine_fuel_resolved  NVARCHAR(100),
    heading               NVARCHAR(MAX),
    car_type              NVARCHAR(100),
    no_of_owners          INT,
    color                 NVARCHAR(50),
    condition             NVARCHAR(50),
    condition_resolved    NVARCHAR(100),
    address               NVARCHAR(255),
    location              NVARCHAR(255),
    postcode              NVARCHAR(20),
    district              NVARCHAR(100),
    state                 NVARCHAR(100),
    country               NVARCHAR(100),
    coordinates           NVARCHAR(50),
    price                 DECIMAL(18, 2),
    price_for_display     NVARCHAR(50),
    warranty              INT,
    warranty_resolved     NVARCHAR(50),
    published             BIGINT,
    published_string      NVARCHAR(50),
    last_updated          BIGINT,
    isprivate             BIT,
    seo_url               NVARCHAR(MAX),
    main_image_url        NVARCHAR(MAX),
    timestamp             DATETIME DEFAULT GETDATE(),
    last_synced           DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.equipment
(
    id                 INT IDENTITY PRIMARY KEY,
    willhaben_id       BIGINT NOT NULL REFERENCES dl.willhaben (id),
    equipment_code     NVARCHAR(MAX),
    equipment_resolved NVARCHAR(MAX),
    timestamp          DATETIME DEFAULT GETDATE(),
    last_synced        DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.make
(
    make_id     INT           NOT NULL PRIMARY KEY,
    make_name   NVARCHAR(255) NOT NULL,
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.model
(
    model_id    INT           NOT NULL PRIMARY KEY,
    model_name  NVARCHAR(255) NOT NULL,
    make_id     INT           NOT NULL REFERENCES dl.make (make_id),
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.engine_effect
(
    id    INT          NOT NULL PRIMARY KEY,
    power NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE dl.engine_fuel
(
    id          INT          NOT NULL PRIMARY KEY,
    fuel_type   NVARCHAR(50) NOT NULL,
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.battery_capacity
(
    id       INT          NOT NULL PRIMARY KEY,
    capacity NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE dl.wltp_range
(
    id    INT          NOT NULL PRIMARY KEY,
    range NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE dl.transmission
(
    id                INT          NOT NULL PRIMARY KEY,
    transmission_type NVARCHAR(50) NOT NULL,
    last_synced       DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.wheel_drive
(
    id         INT          NOT NULL PRIMARY KEY,
    drive_type NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE dl.equipment_search
(
    id             INT           NOT NULL PRIMARY KEY,
    equipment_name NVARCHAR(255) NOT NULL,
    last_synced    DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.exterior_colour_main
(
    id          INT          NOT NULL PRIMARY KEY,
    colour      NVARCHAR(50) NOT NULL,
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.no_of_doors
(
    id         INT NOT NULL PRIMARY KEY,
    door_count INT NOT NULL
);
GO

CREATE TABLE dl.no_of_seats
(
    id         INT NOT NULL PRIMARY KEY,
    seat_count INT NOT NULL
);
GO

CREATE TABLE dl.location
(
    id   INT           NOT NULL PRIMARY KEY,
    name NVARCHAR(255) NOT NULL
);
GO

CREATE TABLE dl.area
(
    id          INT           NOT NULL PRIMARY KEY,
    name        NVARCHAR(255) NOT NULL,
    location_id INT           NOT NULL REFERENCES dl.location (id)
);
GO

CREATE TABLE dl.dealer
(
    id   INT           NOT NULL PRIMARY KEY,
    type NVARCHAR(255) NOT NULL
);
GO

CREATE TABLE dl.periode
(
    id INT NOT NULL PRIMARY KEY,
    PERIOD NVARCHAR(255
) NOT NULL
);
GO

CREATE TABLE dl.car_type
(
    id          INT           NOT NULL PRIMARY KEY,
    type        NVARCHAR(255) NOT NULL,
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.motor_condition
(
    id          INT           NOT NULL PRIMARY KEY,
    condition   NVARCHAR(255) NOT NULL,
    last_synced DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE dl.warranty
(
    id                 INT           NOT NULL PRIMARY KEY,
    warranty_available NVARCHAR(255) NOT NULL
);
GO

--+-----------------------+--
--| Data Warehouse Tables |--
--+-----------------------+--

CREATE TABLE dwh.source
(
    id          INT NOT NULL PRIMARY KEY,
    source_name NVARCHAR(50)
);
GO

INSERT INTO dwh.source (id, source_name)
VALUES (1, 'willhaben'),
       (2, 'gebrauchtwagen');
GO

CREATE TABLE dwh.make
(
    id        INT NOT NULL PRIMARY KEY,
    make_name NVARCHAR(50)
);
GO

CREATE TABLE dwh.model
(
    id         INT           NOT NULL PRIMARY KEY,
    model_name NVARCHAR(255) NOT NULL,
    make_id    INT           NOT NULL REFERENCES dwh.make (id)
);
GO

CREATE TABLE dwh.color
(
    id         INT NOT NULL PRIMARY KEY,
    color_name NVARCHAR(50)
);
GO

CREATE TABLE dwh.equipment_details
(
    equipment_code INT NOT NULL PRIMARY KEY,
    equipment      NVARCHAR(255)
);
GO

CREATE TABLE dwh.transmission
(
    id                INT NOT NULL PRIMARY KEY,
    transmission_type NVARCHAR(50)
);
GO

CREATE TABLE dwh.condition
(
    id            INT NOT NULL PRIMARY KEY,
    car_condition NVARCHAR(50)
);
GO

CREATE TABLE dwh.car_type
(
    id   INT NOT NULL PRIMARY KEY,
    type NVARCHAR(50)
);
GO

CREATE TABLE dwh.fuel
(
    id        INT NOT NULL PRIMARY KEY,
    fuel_type NVARCHAR(50)
);
GO

CREATE TABLE dwh.location
(
    id                  BIGINT IDENTITY PRIMARY KEY,
    willhaben_id        BIGINT,
    address             NVARCHAR(255),
    location            NVARCHAR(255),
    postcode            NVARCHAR(255),
    district            NVARCHAR(255),
    state               NVARCHAR(100),
    country             NVARCHAR(100),
    longitude           NVARCHAR(100),
    latitude            NVARCHAR(100),
    gebrauchtwagen_guid UNIQUEIDENTIFIER
);
GO

CREATE UNIQUE INDEX idx_location_willhabenid_notnull
    ON dwh.location (willhaben_id)
    WHERE [willhaben_id] IS NOT NULL;
GO

CREATE UNIQUE INDEX idx_location_gwguid_notnull
    ON dwh.location (gebrauchtwagen_guid)
    WHERE [gebrauchtwagen_guid] IS NOT NULL;
GO

CREATE TABLE dwh.willwagen
(
    id                     BIGINT IDENTITY PRIMARY KEY,
    willhaben_id           BIGINT,
    gw_guid                UNIQUEIDENTIFIER,
    source_id              INT NOT NULL REFERENCES dwh.source (id),
    make_id                INT REFERENCES dwh.make (id),
    model_id               INT REFERENCES dwh.model (id),
    year_model             INT,
    transmission_id        INT REFERENCES dwh.transmission (id),
    mileage                INT,
    noofseats              INT,
    power_in_kw            INT,
    engine_fuel_id         INT REFERENCES dwh.fuel (id),
    car_type_id            INT REFERENCES dwh.car_type (id),
    no_of_owners           INT,
    color_id               INT REFERENCES dwh.color (id),
    condition_id           INT REFERENCES dwh.condition (id),
    price                  DECIMAL(18, 2),
    predicted_dealer_price DECIMAL(18, 2),
    warranty               BIT,
    published              DATETIME,
    last_updated           DATETIME,
    isprivate              BIT,
    timestamp              DATETIME DEFAULT GETDATE()
);
GO

CREATE UNIQUE INDEX idx_willhaben_id_notnull
    ON dwh.willwagen (willhaben_id)
    WHERE [willhaben_id] IS NOT NULL;
GO

CREATE UNIQUE INDEX idx_willwagen_guid_notnull
    ON dwh.willwagen (gw_guid)
    WHERE [gw_guid] IS NOT NULL;
GO

CREATE TABLE dwh.equipment
(
    id             BIGINT NOT NULL PRIMARY KEY,
    willhaben_id   BIGINT NOT NULL REFERENCES dwh.willwagen (id),
    equipment_code INT REFERENCES dwh.equipment_details (equipment_code)
);
GO

CREATE TABLE dwh.specification
(
    id            BIGINT IDENTITY PRIMARY KEY,
    willhaben_id  BIGINT NOT NULL REFERENCES dwh.willwagen (id),
    specification NVARCHAR(MAX)
);
GO

CREATE TABLE dwh.description
(
    id           BIGINT IDENTITY PRIMARY KEY,
    willhaben_id BIGINT NOT NULL REFERENCES dwh.willwagen (id),
    description  NVARCHAR(MAX)
);
GO

CREATE TABLE dwh.image_url
(
    id           BIGINT IDENTITY PRIMARY KEY,
    willhaben_id BIGINT NOT NULL REFERENCES dwh.willwagen (id),
    image_url    NVARCHAR(MAX)
);
GO

CREATE TABLE dwh.seo_url
(
    id           BIGINT IDENTITY PRIMARY KEY,
    willhaben_id BIGINT NOT NULL REFERENCES dwh.willwagen (id),
    seo_url      NVARCHAR(MAX)
);
GO

CREATE TABLE dwh.sync_log
(
    table_name     NVARCHAR(100) NOT NULL PRIMARY KEY,
    last_sync_time DATETIME
);
GO

--+----------------+--
--| Willhaben View |--
--+----------------+--

CREATE VIEW dwh.willhaben AS
SELECT ww.willhaben_id,
       m.make_name,
       m2.model_name,
       s2.specification AS specification,
       d.description    AS description,
       ww.year_model,
       t.transmission_type,
       ww.mileage,
       ww.noofseats,
       ww.power_in_kw,
       f.fuel_type,
       ct.type,
       ww.no_of_owners,
       c.color_name,
       c2.car_condition,
       l.address        AS address,
       l.location       AS location,
       l.postcode,
       l.district,
       l.state,
       l.country,
       l.latitude,
       l.longitude,
       ww.price,
       ww.predicted_dealer_price,
       ww.warranty,
       ww.isprivate,
       iu.image_url,
       u.seo_url,
       ww.published,
       ww.last_updated
FROM dwh.willwagen ww
         JOIN make m ON m.id = ww.make_id
         JOIN model m2 ON m2.id = ww.model_id
         JOIN specification s2 ON s2.willhaben_id = ww.willhaben_id
         JOIN description d ON d.willhaben_id = ww.willhaben_id
         JOIN transmission t ON t.id = ww.transmission_id
         JOIN fuel f ON f.id = ww.engine_fuel_id
         JOIN car_type ct ON ct.id = ww.car_type_id
         JOIN color c ON c.id = ww.color_id
         JOIN condition c2 ON c2.id = ww.condition_id
         JOIN seo_url u ON u.willhaben_id = ww.willhaben_id
         JOIN image_url iu ON iu.willhaben_id = ww.willhaben_id
         JOIN location l ON l.willhaben_id = ww.willhaben_id
WHERE ww.source_id = 1;
GO
