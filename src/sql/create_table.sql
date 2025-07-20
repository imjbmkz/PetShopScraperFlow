DROP TABLE IF EXISTS shops;
CREATE TABLE shops (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime DEFAULT now()
    ,name varchar(50) CHARACTER SET utf8mb4
    ,base_url varchar(100) CHARACTER SET utf8mb4
);

INSERT INTO shops (name, base_url)
VALUES 
    ('Zooplus','https://www.zooplus.co.uk/')
    ,('PetsAtHome','https://www.petsathome.com/')
    ,('PetPlanet','https://www.petplanet.co.uk/')
    ,('PetSupermarket','https://www.pet-supermarket.co.uk/')
    ,('PetDrugsOnline','https://www.petdrugsonline.co.uk/')
    ,('Jollyes','https://www.jollyes.co.uk/')
    ,('LilysKitchen','https://www.lilyskitchen.co.uk/')
    ,('Viovet','https://www.viovet.co.uk/')
    ,('Bitiba','https://www.bitiba.co.uk/')
    ,('DirectVet','https://www.direct-vet.co.uk/')
    ,('Fishkeeper','https://www.fishkeeper.co.uk/')
    ,('Purina','https://www.purina.co.uk/')
    ,('PetShop', 'https://www.petshop.co.uk/')
    ,('Vetshop','https://www.vetshop.co.uk/')
    ,('VetUK','https://www.vetuk.co.uk/')
    ,('Burnspet','https://burnspet.co.uk/')
    ,('ASDAGroceries','https://groceries.asda.com/')
    ,('TheRange','https://www.therange.co.uk/')
    ,('Ocado','https://www.ocado.com/')
    ,('Harringtons','https://www.harringtonspetfood.com/')
    ,('BernPetFoods','https://www.bernpetfoods.co.uk/')
    ,('PetsCorner','https://www.petscorner.co.uk/')
    ,('Orijen','https://www.orijenpetfoods.co.uk/')
    ,('ThePetExpress','https://www.thepetexpress.co.uk/')
    ,('PetShopOnline','https://pet-shop-online.co.uk/')
    ,('TaylorPetFoods','https://www.taylorspetfoods.co.uk/')
    ,('TheNaturalPetStore','https://www.thenaturalpetstore.co.uk/')
    ,('HealthyPetStore','https://healthypetstore.co.uk/')
    ,('FarmAndPetPlace','https://www.farmandpetplace.co.uk/')
    ,('NaturesMenu','https://www.naturesmenu.co.uk/');

DROP TABLE IF EXISTS stg_urls;
CREATE TABLE stg_urls (
    shop varchar(50) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,updated_date datetime
);

DROP TABLE IF EXISTS urls;
CREATE TABLE urls (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY
    ,inserted_date datetime DEFAULT now()
    ,shop varchar(50) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,scrape_status varchar(25) CHARACTER SET utf8mb4 DEFAULT 'NOT STARTED'
    ,updated_date datetime
);

DROP TABLE IF EXISTS stg_pet_products;
CREATE TABLE stg_pet_products (
    shop varchar(50) CHARACTER SET utf8mb4
    ,name varchar(255) CHARACTER SET utf8mb4
    ,rating varchar(50) CHARACTER SET utf8mb4
    ,description varchar(1000) CHARACTER SET utf8mb4
    ,url varchar(255) CHARACTER SET utf8mb4
    ,variant varchar(255) CHARACTER SET utf8mb4
    ,image_urls varchar(1000) CHARACTER SET utf8mb4
    ,price decimal(10, 4)
    ,discounted_price decimal(10, 4)
    ,discount_percentage decimal(10, 4)
);

DROP TABLE IF EXISTS pet_product_variants;
CREATE TABLE pet_product_variants (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    shop_id INT NOT NULL,
    inserted_date DATETIME DEFAULT NOW(),
    url VARCHAR(255) CHARACTER SET utf8mb4,
    variant TEXT CHARACTER SET utf8mb4,
    image_urls TEXT CHARACTER SET utf8mb4
);


DROP TABLE IF EXISTS pet_product_variant_prices;
CREATE TABLE pet_product_variant_prices (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_variant_id INT NOT NULL,
    shop_id INT NOT NULL,
    inserted_date DATETIME DEFAULT NOW(),
    price DECIMAL(10, 4),
    discounted_price DECIMAL(10, 4),
    discount_percentage DECIMAL(10, 4)
);


DROP TABLE IF EXISTS pet_products;
CREATE TABLE pet_products (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    inserted_date DATETIME DEFAULT NOW(),
    shop_id INT NOT NULL,
    name VARCHAR(255) CHARACTER SET utf8mb4,
    rating VARCHAR(50) CHARACTER SET utf8mb4,
    description TEXT CHARACTER SET utf8mb4,
    url VARCHAR(255) CHARACTER SET utf8mb4,
    image_urls TEXT CHARACTER SET utf8mb4 
);


ALTER TABLE pet_products 
ADD FOREIGN KEY (shop_id) REFERENCES shops(id);

ALTER TABLE pet_product_variants 
ADD FOREIGN KEY (shop_id) REFERENCES shops(id);

ALTER TABLE pet_product_variant_prices 
ADD FOREIGN KEY (shop_id) REFERENCES shops(id);

ALTER TABLE pet_product_variant_prices 
ADD FOREIGN KEY (product_variant_id) REFERENCES pet_product_variants(id);