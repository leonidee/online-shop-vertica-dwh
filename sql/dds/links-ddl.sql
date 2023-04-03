/*
 ORDER -> CUSTOMER
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_order_customer CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_order_customer
(
    order_id    bigint       NOT NULL,
    customer_id bigint       NOT NULL,
    load_dttm   timestamp(0) NOT NULL,
    load_src    varchar(50)  NOT NULL,

    FOREIGN KEY (order_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders (id),
    FOREIGN KEY (customer_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers (id),
    PRIMARY KEY (order_id, customer_id)
)
    ORDER BY order_id
    SEGMENTED BY hash(order_id, customer_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);
/*
 CUSTOMER -> GEOLOCATION
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_customer_geolocation CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_customer_geolocation
(
    customer_id bigint       NOT NULL,
    zip_code    int          NOT NULL,
    load_dttm   timestamp(0) NOT NULL,
    load_src    varchar(50)  NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers (id)
)
    ORDER BY customer_id
    SEGMENTED BY hash(customer_id, zip_code) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 GEOLOCATION -> CITY
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_geolocation_city CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_geolocation_city
(
    id        bigint       NOT NULL PRIMARY KEY ENABLED,
    city_id   bigint       NOT NULL,
    zip_code  int          NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL,

    FOREIGN KEY (city_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities (id)
)
    ORDER BY zip_code
    SEGMENTED BY id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 SELLER -> GEOLOCATION
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_seller_geolocation CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_seller_geolocation
(
    seller_id bigint       NOT NULL,
    zip_code  int          NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL,

    FOREIGN KEY (seller_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers (id)
)
    ORDER BY zip_code
    SEGMENTED BY hash(seller_id, zip_code) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 PRODUCT -> SELLER
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_product_seller CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_product_seller
(
    product_id bigint       NOT NULL,
    seller_id  bigint       NOT NULL,
    load_dttm  timestamp(0) NOT NULL,
    load_src   varchar(50)  NOT NULL,

    FOREIGN KEY (product_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_products (id),
    FOREIGN KEY (seller_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers (id),
    PRIMARY KEY (product_id, seller_id)
)
    ORDER BY seller_id
    SEGMENTED BY hash(product_id, seller_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);


/*
 PRODUCT -> CATEGORY
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_product_category CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_product_category
(
    product_id  bigint       NOT NULL,
    category_id bigint       NOT NULL,
    load_dttm   timestamp(0) NOT NULL,
    load_src    varchar(50)  NOT NULL,

    FOREIGN KEY (product_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_products (id),
    FOREIGN KEY (category_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories (id)
)
    ORDER BY category_id
    SEGMENTED BY hash(product_id, category_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 REVIEW -> ORDER
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_review_order CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_review_order
(
    review_id bigint       NOT NULL,
    order_id  bigint       NOT NULL,
    load_dttm timestamp(0) NOT NULL,
    load_src  varchar(50)  NOT NULL,

    FOREIGN KEY (review_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews (id),
    FOREIGN KEY (order_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders (id),
    PRIMARY KEY (review_id, order_id)
)
    ORDER BY order_id
    SEGMENTED BY hash(review_id, order_id) ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);

/*
 ORDER -> ITEMS
 */
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_order_items CASCADE;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_order_items
(
    order_item_id  bigint         NOT NULL PRIMARY KEY ENABLED,
    product_id     bigint         NOT NULL,
    order_id       bigint         NOT NULL,
    order_item_num int,
    price          numeric(10, 2) NOT NULL,
    freight_value  numeric(10, 2),
    load_dttm      timestamp(0)   NOT NULL,
    load_src       varchar(50)    NOT NULL,

    FOREIGN KEY (product_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_products (id),
    FOREIGN KEY (order_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders (id)
)
    ORDER BY order_id
    SEGMENTED BY order_item_id ALL NODES
    PARTITION BY load_dttm::date
        GROUP BY calendar_hierarchy_day(load_dttm::date, 3, 2);






















