from shops import (
    AsdaETL,
    BernPetFoodsETL,
    BitibaETL,
    BurnsPetETL,
    DirectVetETL,
    FarmAndPetPlaceETL,
    FishKeeperETL,
    HarringtonsETL,
    HealthyPetStoreETL,
    JollyesETL,
    LilysKitchenETL,
    NaturesMenuETL,
    OcadoETL,
    OrijenETL,
    PetDrugsOnlineETL,
    PetPlanetETL,
    PetShopETL,
    PetShopOnlineETL,
    PetSupermarketETL,
    PetsAtHomeETL,
    PetsCornerETL,
    PurinaETL,
    TaylorPetFoodsETL,
    TheNaturalPetStoreETL,
    ThePetExpressETL,
    TheRangeETL,
    VetShopETL,
    VetUKETL,
    ViovetETL,
    ZooplusETL
)


SHOPS = {
    "ASDAGroceries": AsdaETL(),
    "BernPetFoods": BernPetFoodsETL(),
    "Bitiba": BitibaETL(),
    "BurnsPet": BurnsPetETL(),
    "DirectVet": DirectVetETL(),
    "FarmAndPetPlace": FarmAndPetPlaceETL(),
    "FishKeeper": FishKeeperETL(),
    "Harringtons": HarringtonsETL(),
    "HealthyPetStore": HealthyPetStoreETL(),
    "Jollyes": JollyesETL(),
    "LilysKitchen": LilysKitchenETL(),
    "NaturesMenu": NaturesMenuETL(),
    "Ocado": OcadoETL(),
    "Orijen": OrijenETL(),
    "PetDrugsOnline": PetDrugsOnlineETL(),
    "PetPlanet": PetPlanetETL(),
    "PetShop": PetShopETL(),
    "PetShopOnline": PetShopOnlineETL(),
    "PetSupermarket": PetSupermarketETL(),
    "PetsAtHome": PetsAtHomeETL(),
    "PetsCorner": PetsCornerETL(),
    "Purina": PurinaETL(),
    "TaylorPetFoods": TaylorPetFoodsETL(),
    "TheNaturalPetStore": TheNaturalPetStoreETL(),
    "ThePetExpress": ThePetExpressETL(),
    "TheRange": TheRangeETL(),
    "VetShop": VetShopETL(),
    "VetUK": VetUKETL(),
    "Viovet": ViovetETL(),
    "Zooplus": ZooplusETL(),
}


def run_etl(shop: str):
    if shop in SHOPS:
        return SHOPS[shop]
    else:
        raise ValueError(
            f"Shop {shop} is not supported. Please pass a valid shop.")
