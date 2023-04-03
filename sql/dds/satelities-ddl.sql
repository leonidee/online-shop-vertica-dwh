/*
 CUSTOMERS GEOLOCATION
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_geolocation CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_geolocation
(
    customer_id bigint       NOT NULL,
    zip_code    int          NOT NULL,
    valid_from  timestamp(0) NOT NULL,
    valid_to    timestamp(0) NULL,
    load_dttm   timestamp(0) NOT NULL,
    load_src    varchar(50)  NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers (id)
)
    ORDER BY customer_id
    SEGMENTED BY customer_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 CUSTOMERS INFO
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_info CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_info
(
    customer_id        bigint       NOT NULL PRIMARY KEY ENABLED,
    customer_unique_id varchar(100) NOT NULL,
    customer_name      varchar(100) NOT NULL,
    date_of_birth      date         NULL,
    load_dttm          timestamp(0) NOT NULL,
    load_src           varchar(50)  NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers (id)
)
    ORDER BY customer_id
    SEGMENTED BY customer_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 CUSTOMERS CONTACTS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_contacts CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_contacts
(
    customer_id  bigint       NOT NULL PRIMARY KEY ENABLED,
    email        varchar(50)  NOT NULL,
    phone_number varchar(50)  NULL,
    load_dttm    timestamp(0) NOT NULL,
    load_src     varchar(50)  NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers (id)
)
    ORDER BY customer_id
    SEGMENTED BY customer_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 REVIEWS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_reviews CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_reviews
(
    review_id        bigint       NOT NULL PRIMARY KEY ENABLED,
    review_score     int          NOT NULL,
    comment_title    varchar(100) NULL,
    comment_message  varchar(300) NULL,
    creation_date    timestamp(0) NULL,
    answer_timestamp timestamp(0) NULL,
    is_commented     boolean      NOT NULL,
    load_dttm        timestamp(0) NOT NULL,
    load_src         varchar(50)  NOT NULL,

    FOREIGN KEY (review_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews (id)
)
    ORDER BY review_id
    SEGMENTED BY review_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 CATEGORY
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_category CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_category
(
    category_id          bigint       NOT NULL PRIMARY KEY ENABLED,
    category_name        varchar(100) NOT NULL,
    category_name_origin varchar(100) NOT NULL,
    load_dttm            timestamp(0) NOT NULL,
    load_src             varchar(50)  NOT NULL,

    FOREIGN KEY (category_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories (id)
)
    ORDER BY category_id
    SEGMENTED BY category_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 PRODUCT NAMES
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_names CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_names
(
    product_id   bigint       NOT NULL PRIMARY KEY ENABLED,
    product_name varchar(100) NOT NULL,
    load_dttm    timestamp(0) NOT NULL,
    load_src     varchar(50)  NOT NULL,

    FOREIGN KEY (product_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_products (id)
)
    ORDER BY product_id
    SEGMENTED BY product_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 PRODUCTS DIMENSIONS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_dimensions CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_dimensions
(
    product_id bigint       NOT NULL PRIMARY KEY ENABLED,
    weight_g   float        NULL,
    length_cm  float        NULL,
    height_cm  float        NULL,
    width_cm   float        NULL,
    load_dttm  timestamp(0) NOT NULL,
    load_src   varchar(50)  NOT NULL,

    FOREIGN KEY (product_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_products (id)
)
    ORDER BY product_id
    SEGMENTED BY product_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 SELLERS INFO
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_sellers_info CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_sellers_info
(
    seller_id    bigint       NOT NULL PRIMARY KEY ENABLED,
    email        varchar(100),
    phone_number varchar(100),
    load_dttm    timestamp(0) NOT NULL,
    load_src     varchar(50)  NOT NULL,

    FOREIGN KEY (seller_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers (id)
)
    ORDER BY seller_id
    SEGMENTED BY seller_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 ORDER STATUSES
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_order_statuses CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_order_statuses
(
    order_id     bigint       NOT NULL,
    order_status varchar(100) NOT NULL,
    valid_from   timestamp(0) NOT NULL,
    valid_to     timestamp(0) NULL,
    load_dttm    timestamp(0) NOT NULL,
    load_src     varchar(50)  NOT NULL,

    FOREIGN KEY (order_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders (id)
)
    ORDER BY order_id
    SEGMENTED BY order_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 ORDER DETAILS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_order_details CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_order_details
(
    order_id                      bigint       NOT NULL PRIMARY KEY ENABLED,
    order_purchase_timestamp      timestamp(0) NOT NULL,
    order_approved_at             timestamp(0) NULL,
    order_delivered_carrier_date  timestamp(0) NULL,
    order_delivered_customer_date timestamp(0) NULL,
    order_estimated_delivery_date timestamp(0) NULL,
    load_dttm                     timestamp(0) NOT NULL,
    load_src                      varchar(50)  NOT NULL,

    FOREIGN KEY (order_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders (id)
)
    ORDER BY order_id
    SEGMENTED BY order_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 COORDINATES
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_coordinates CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_coordinates
(
    geolocation_id bigint       NOT NULL PRIMARY KEY ENABLED,
    latitude       float        NOT NULL,
    longitude      float        NOT NULL,
    load_dttm      timestamp(0) NOT NULL,
    load_src       varchar(50)  NOT NULL,

    FOREIGN KEY (geolocation_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_geolocations (id)
)
    ORDER BY geolocation_id
    SEGMENTED BY geolocation_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 CITIES_INFO
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.sat_cities_info CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.sat_cities_info
(
    city_id   bigint       NOT NULL PRIMARY KEY ENABLED,
    city_name varchar(100) NOT NULL,
    state     varchar(100) NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL,

    FOREIGN KEY (city_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities (id)
)
    ORDER BY city_id
    SEGMENTED BY city_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);
