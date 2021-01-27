
## JavaConfiguration

CREATE TABLE IF NOT EXISTS t_order (
    order_id BIGINT NOT NULL AUTO_INCREMENT, 
    user_id INT NOT NULL, 
    address_id BIGINT NOT NULL, 
    status VARCHAR(50), 
    PRIMARY KEY (order_id)
)

CREATE TABLE IF NOT EXISTS t_order_item(
    order_item_id BIGINT NOT NULL AUTO_INCREMENT, 
    order_id BIGINT NOT NULL, 
    user_id INT NOT NULL, 
    status VARCHAR(50), 
    PRIMARY KEY (order_item_id)
)

CREATE TABLE IF NOT EXISTS t_address (
    address_id BIGINT NOT NULL, 
    address_name VARCHAR(100) NOT NULL, 
    PRIMARY KEY (address_id)
)