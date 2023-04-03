/*
 CUSTOMERS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers
(
    id          bigint       NOT NULL PRIMARY KEY ENABLED,
    customer_id varchar(100) NOT NULL,
    load_dttm   timestamp    NOT NULL,
    load_src    varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY hash(customer_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);
/*
 ORDERS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    order_id  varchar(100) NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY hash(order_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 PRODUCTS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_products CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_products
(
    id         bigint       NOT NULL PRIMARY KEY ENABLED,
    product_id varchar(100) NOT NULL,
    load_dttm  timestamp(0) NOT NULL,
    load_src   varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY hash(product_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 CATEGORIES
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 SELLERS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    seller_id varchar(100) NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY hash(seller_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 REVIEWS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    review_id varchar(100) NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY hash(review_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 GEOLOCATIONS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_geolocations CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_geolocations
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 CITIES
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL
)
    ORDER BY id
    SEGMENTED BY id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);
