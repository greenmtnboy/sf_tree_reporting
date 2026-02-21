#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow", "requests", "pillow", "instructor[litellm]", "duckdb", "google-genai", "jsonref"]
# ///

import sys
import math
import base64
import pyarrow as pa
import pyarrow.compute as pc
import requests
import duckdb
from datetime import datetime, timezone
from typing import Literal, Optional
from PIL import Image, ImageDraw, ImageFilter
from pydantic import BaseModel, Field
import instructor

ICON_SIZE = 48
ENRICHMENT_PARQUET = "https://storage.googleapis.com/trilogy_public_models/duckdb/sf_trees/tree_enrichment.parquet"
TREE_INFO_PARQUET = "https://storage.googleapis.com/trilogy_public_models/duckdb/sf_trees/tree_info.parquet"


SYNONYMS = {
    "brisbane box": "Lophostemon confertus",
    "tree": "Tree",
    "abutilon hybridum": "Abutilon",
    "acacia baileyana 'purpurea'": "Acacia baileyana",
    "acacia spp": "Acacia",
    "acer ginnela": "Acer ginnala",
    "acer palmatum 'bloodgood'": "Acer palmatum",
    "acer palmatum 'sango kaku'": "Acer palmatum",
    "acer platanoides 'crimson king'": "Acer platanoides",
    "acer rubrum 'armstrong'": "Acer rubrum",
    "acer rubrum 'autumn glory'": "Acer rubrum",
    "acer rubrum 'october glory'": "Acer rubrum",
    "acer rubrum 'red sunset'": "Acer rubrum",
    "acer spp": "Maple",
    "acer x 'autumn blaze'": "Acer × freemanii",
    "acer x freemanii 'autumn blaze'": "Acer × freemanii",
    "aesculus spp": "Aesculus",
    "aesculus x carnea 'briotii'": "Aesculus × carnea",
    "aesculus x carnea 'o'neill'": "Aesculus × carnea",
    "agonis flexuosa 'after dark'": "Agonis flexuosa",
    "albizia distachya": "Paraserianthes lophantha",
    "angohpora spp.": "Angophora",
    "arbutus 'marina'": "Arbutus",
    "arbutus unedo 'compacta'": "Arbutus unedo",
    "arctostaphylos manzanita 'dr hurd'": "Arctostaphylos manzanita",
    "bambusa spp": "Bamboo",
    "betula spp": "Birch",
    "brahea aramata": "Brahea armata",
    "brahea brandegeei": "Brahea brandegeei",
    "brugmansia spp": "Brugmansia",
    "callistemon 'jeffers'": "Callistemon",
    "carpinus betulus 'fastigiata'": "Carpinus betulus",
    "caryota maxima 'himalaya'": "Caryota",
    "casurina stricta": "Allocasuarina verticillata",
    "ceanothus 'ray hartman'": "Ceanothus",
    "ceanothus sps": "Ceanothus",
    "cedrus atlantica glauca": "Cedrus atlantica",
    "cercis canadensis 'forest pansy'": "Cercis canadensis",
    "cercis canadensis 'oklahoma'": "Cercis canadensis",
    "chionanthus retusa": "Chionanthus retusus",
    "citrus aurantifolia 'bearss'": "Persian lime",
    "citrus spp": "Citrus",
    "citrus x limon 'lisbon'": "Lemon",
    "citrus x meyeri 'improved'": "Meyer lemon",
    "cornus spp": "Cornus (genus)",
    "cornus nuttallii x florida 'eddie's white wonder'": "Cornus × elwinmoorei",
    "cotinus coggygria 'royal purple'": "Cotinus coggygria",
    "cotoneaster spp": "Cotoneaster",
    "crataegus laevigata 'paul's scarlet'": "Crataegus laevigata",
    "crateagus spp": "Crataegus",
    "cupressus spp": "Cupressus",
    "dodonaea viscosa 'purpurea'": "Dodonaea viscosa",
    "dypsis cabadae": "Dypsis cabadae",
    "eriobotrya deflexa 'coppertone'": "Eriobotrya deflexa",
    "eucalyptus spp": "Eucalyptus",
    "eucalyptus lehmanni": "Eucalyptus lehmannii",
    "eucalyptus leucoxylon mac 'rosea'": "Eucalyptus leucoxylon",
    "eucalyptus macarthuri": "Eucalyptus macarthurii",
    "eucalyptus simmondsi": "Eucalyptus simmondsii",
    "fagus sylvatica 'red obelisk'": "Fagus sylvatica",
    "ficus spp.": "Ficus",
    "ficus carica 'black mission'": "Black Mission fig",
    "ficus carica 'brown turkey'": "Brown Turkey fig",
    "ficus laurel": "Ficus microcarpa",
    "ficus microcarpa 'retusa'": "Ficus microcarpa",
    "ficus microcarpa nitida 'green gem'": "Ficus microcarpa",
    "ficus retusa nitida": "Ficus microcarpa",
    "fraxinus americana 'autumn purple'": "Fraxinus americana",
    "fraxinus holotricha": "Fraxinus holotricha",
    "fraxinus oxycarpa 'raywood'": "Fraxinus angustifolia",
    "fraxinus spp": "Ash",
    "fraxinus uhdei 'tomlinson'": "Fraxinus uhdei",
    "fraxinus velutina 'glabra'": "Fraxinus velutina",
    "fraxinus velutina 'modesto'": "Fraxinus velutina",
    "fraxinus x moraine": "Fraxinus holotricha",
    "fremontodendron spp": "Fremontodendron",
    "garrya elliptica 'evie'": "Garrya elliptica",
    "garrya elliptica 'james roof'": "Garrya elliptica",
    "geijera spp": "Geijera",
    "ginkgo biloba 'autumn gold'": "Ginkgo biloba",
    "ginkgo biloba 'autumn sentinel'": "Ginkgo biloba",
    "ginkgo biloba 'fairmont'": "Ginkgo biloba",
    "ginkgo biloba 'princeton sentry'": "Ginkgo biloba",
    "ginkgo biloba 'saratoga'": "Ginkgo biloba",
    "gleditsia triacanthos 'aurea'": "Honey locust",
    "gleditsia triacanthos 'shademaster'": "Honey locust",
    "gleditsia triacanthos 'sunburst'": "Honey locust",
    "grevillea spp": "Grevillea",
    "ilex altaclarensis 'wilsonii'": "Ilex × altaclerensis",
    "ilex spp": "Ilex",
    "juglans 'paradox'": "Juglans",
    "juniperus scopulorum 'pat": "Juniperus scopulorum",
    "lagerstroemia indica 'natchez'": "Lagerstroemia indica",
    "lagerstroemia indica 'tuscarora'": "Lagerstroemia indica",
    "lagerstroemia spp": "Lagerstroemia",
    "lagerstroemia x 'tuscarora'": "Lagerstroemia",
    "laurus x 'saratoga'": "Laurus nobilis",
    "leptospermum quinquenervia": "Melaleuca quinquenervia",
    "leptospermum scoparium 'helene strybing'": "Leptospermum scoparium",
    "leptospermum scoparium 'ruby glow'": "Leptospermum scoparium",
    "leptospermum scoparium 'snow white'": "Leptospermum scoparium",
    "leucadendron 'gold strike'": "Leucadendron",
    "leucodendron argenteum": "Leucadendron argenteum",
    "liquidambar styraciflua 'burgundy'": "Liquidambar styraciflua",
    "liquidambar styraciflua 'festival'": "Liquidambar styraciflua",
    "liquidambar styraciflua 'palo alto'": "Liquidambar styraciflua",
    "liquidambar styraciflua 'rotundiloba'": "Liquidambar styraciflua",
    "liquidambar styraciflua 'slender silhoutte'": "Liquidambar styraciflua",
    "lyonothamnus floribundus subsp. asplenifolius": "Lyonothamnus floribundus",
    "magnolia doltsopa 'silvercloud'": "Magnolia doltsopa",
    "magnolia grandiflora 'd.d. blanchard'": "Magnolia grandiflora",
    "magnolia grandiflora 'little gem'": "Magnolia grandiflora",
    "magnolia grandiflora 'majestic beauty'": "Magnolia grandiflora",
    "magnolia grandiflora 'russet'": "Magnolia grandiflora",
    "magnolia grandiflora 'saint mary'": "Magnolia grandiflora",
    "magnolia grandiflora 'samuel sommer'": "Magnolia grandiflora",
    "magnolia grandiflora 'timeless beauty'": "Magnolia grandiflora",
    "magnolia sargentiana 'robusta'": "Magnolia sargentiana",
    "magnolia spp": "Magnolia",
    "magnolia x foggii 'jack fogg'": "Magnolia × foggii",
    "magnolia x soulangiana 'rustica rubra'": "Magnolia × soulangiana",
    "magnolia x soulangiana": "Magnolia × soulangiana",
    "malus 'gala'": "Gala (apple)",
    "malus floribunda 'prairie fire'": "Malus floribunda",
    "malus x 'callaway'": "Malus",
    "maytenus boaria 'green showers'": "Maytenus boaria",
    "melaleuca spp": "Melaleuca",
    "melaleuca styphelliodes": "Melaleuca styphelioides",
    "melia azerdarach": "Melia azedarach",
    "metasequoia glyplostroboides": "Metasequoia glyptostroboides",
    "metrosideros excelsa 'aurea'": "Metrosideros excelsa",
    "metrosideros spp": "Metrosideros",
    "michelia champaca 'alba'": "Magnolia × alba",
    "morus alba 'fruitless'": "Morus alba",
    "new zealand tea tree": "Leptospermum scoparium",
    "olea majestic beauty": "Olea europaea",
    "olea europaea 'fruitless'": "Olea europaea",
    "olea europaea 'majestic beauty'": "Olea europaea",
    "olea europaea 'swan hill'": "Olea europaea",
    "olea europaea 'wilsonii'": "Olea europaea",
    "palm (unknown genus)": "Arecaceae",
    "persea americana 'stewart'": "Avocado",
    "phoenix spp": "Phoenix (plant)",
    "picea spp": "Spruce",
    "pinus spp": "Pine",
    "pinus thunbergii 'thunderhead'": "Pinus thunbergii",
    "pittosporum spp": "Pittosporum",
    "platanus x hispanica 'bloodgood'": "Platanus × acerifolia",
    "platanus x hispanica 'columbia'": "Platanus × acerifolia",
    "platanus x hispanica 'yarwood'": "Platanus × acerifolia",
    "podocarpus gracilor": "Afrocarpus gracilior",
    "populus spp": "Populus",
    "prunus cerasifera 'atropurpurea'": "Prunus cerasifera",
    "prunus cerasifera 'krauter vesuvius'": "Prunus cerasifera",
    "prunus cerasifera 'thundercloud'": "Prunus cerasifera",
    "prunus domestica 'green gage'": "Greengage",
    "prunus domestica 'mariposa'": "Prunus domestica",
    "prunus domestica 'santa rosa'": "Prunus domestica",
    "prunus ilicifoia": "Prunus ilicifolia",
    "prunus persica nectarina": "Nectarine",
    "prunus sargentii 'columnaris'": "Prunus sargentii",
    "prunus serrulata 'akebono'": "Prunus × yedoensis",
    "prunus serrulata 'amanagawa'": "Prunus serrulata",
    "prunus serrulata 'double pink weeping'": "Prunus serrulata",
    "prunus serrulata 'mt. fuji'": "Prunus serrulata",
    "prunus serrulata 'royal burgundy'": "Prunus serrulata",
    "prunus spp 'purpurea'": "Prunus cerasifera",
    "prunus spp": "Prunus",
    "prunus subhirtella 'autumnalis'": "Prunus subhirtella",
    "prunus x 'amanogawa'": "Prunus serrulata",
    "prunus x yedoensis 'akebono'": "Prunus × yedoensis",
    "punica granatum 'wonderfu": "Pomegranate",
    "pyracantha 'santa cruz'": "Pyracantha",
    "pyrus calleryana 'aristocrat'": "Pyrus calleryana",
    "pyrus calleryana 'bradford'": "Pyrus calleryana",
    "pyrus calleryana 'capital'": "Pyrus calleryana",
    "pyrus calleryana 'chanticleer'": "Pyrus calleryana",
    "pyrus calleryana 'cleveland'": "Pyrus calleryana",
    "pyrus calleryana 'new bradford'": "Pyrus calleryana",
    "pyrus calleryana 'redspire'": "Pyrus calleryana",
    "pyrus pyrifolia '20th century'": "Pyrus pyrifolia",
    "pyrus pyrifolia 'sainseiki'": "Pyrus pyrifolia",
    "pyrus spp": "Pyrus",
    "pyrus x 'bartlett'": "Williams pear",
    "quercus frainetto 'trump'": "Quercus frainetto",
    "quercus keloggii": "Quercus kelloggii",
    "quercus spp": "Oak",
    "rhamnus alaternus 'john edwards'": "Rhamnus alaternus",
    "rhaphiolepis majestic beauty": "Rhaphiolepis",
    "robinia pseudoacacia 'umbraculifera'": "Robinia pseudoacacia",
    "robinia x ambigua 'idahoensis'": "Robinia × ambigua",
    "robinia x ambigua 'purple robe'": "Robinia × ambigua",
    "robinia x ambigua": "Robinia × ambigua",
    "salix matsudana 'tortuosa'": "Salix matsudana",
    "salix spp": "Willow",
    "sambucus species": "Sambucus",
    "schefflera species": "Schefflera",
    "solanum rantonnetti": "Lycianthes rantonnetii",
    "sophora japonica 'regent'": "Styphnolobium japonicum",
    "syagrus romanzoffianum": "Syagrus romanzoffiana",
    "thuja occidentalis 'emerald'": "Thuja occidentalis",
    "tibochina urvilleana": "Tibouchina urvilleana",
    "tilia americana 'redmond'": "Tilia americana",
    "tilia spp": "Tilia",
    "tree(s)": "Tree",
    "tristaniopsis laurina 'elegant'": "Tristaniopsis laurina",
    "ulmus carpinifolia 'frontier'": "Ulmus",
    "ulmus parvifolia 'athena'": "Ulmus parvifolia",
    "ulmus propinqua 'emerald sunshine'": "Ulmus propinqua",
    "ulmus spp": "Elm",
    "viburnum odoratissimum var. awabuki": "Viburnum odoratissimum",
    "yucca spp": "Yucca",
    "zelkova serrata 'village green'": "Zelkova serrata",
    "patanus racemosa": "Platanus racemosa",
    "x chiranthofremontia lenzii": "× Chiranthofremontia",
    "brahea brandegeei": "Brahea",
    "caryota maxima 'himalaya'": "Caryota maxima",
    "citrus × limon 'lisbon'": "Lemon",
    "citrus × meyeri 'improved'": "Meyer lemon",
    "cornus nuttallii x florida 'eddie's white wonder'": "Cornus nuttallii",
    "dypsis cabadae": "Dypsis",
    "eucalyptus simmondsi": "Eucalyptus simmondsii",
    "ficus carica 'black mission'": "Common fig",
    "ficus carica 'brown turkey'": "Common fig",
    "fraxinus holotricha": "Fraxinus",
    "fraxinus x moraine": "Fraxinus americana",
    "leptospermum scoparium 'helene strybing'": "Leptospermum scoparium",
    "magnolia x foggii 'jack fogg'": "Magnolia × foggii",
    "pyracantha 'santa cruz'": "Pyracantha",
    "robinia x ambigua 'idahoensis'": "Robinia",
    "robinia x ambigua 'purple robe'": "Robinia",
    "robinia x ambigua": "Robinia × ambigua",
    "x chiranthofremontia lenzii": "× Chiranthofremontia",
}

EXCLUDED_SPECIES = {"::", "tree", "to be determine'd"}


def should_skip_species(species: str) -> bool:
    return species.strip().lower() in EXCLUDED_SPECIES

# ── Pydantic model ─────────────────────────────────────────────────────────────

class TreeEnrichment(BaseModel):
    common_names: list[str] = Field(
        description="All known common names for this species, most familiar first"
    )
    native_status: Literal["native_bay_area", "native_california", "non_native", "naturalized", "unknown"] = Field(
        description=(
            "Native status relative to the San Francisco Bay Area. "
            "'native_bay_area' = native to the SF Bay Area specifically. "
            "'native_california' = native to California but not the Bay Area. "
            "'naturalized' = non-native but now established in the wild. "
            "'non_native' = introduced, not naturalized."
        )
    )
    is_evergreen: Optional[bool] = Field(
        None, description="True if evergreen, False if deciduous, None if unknown or semi-evergreen"
    )
    mature_height_ft: Optional[float] = Field(
        None, description="Typical mature height in feet. Use midpoint if a range is given."
    )
    canopy_spread_ft: Optional[float] = Field(
        None, description="Typical mature canopy spread in feet. Use midpoint if a range is given."
    )
    growth_rate: Optional[Literal["slow", "moderate", "fast"]] = Field(None)
    lifespan_years: Optional[str] = Field(
        None, description="Typical lifespan, e.g. '50-100', '200+', 'short-lived'"
    )
    drought_tolerance: Optional[Literal["low", "moderate", "high"]] = Field(None)
    bloom_season: Optional[str] = Field(
        None, description="Season or months when it blooms, e.g. 'spring', 'March-May', 'year-round'"
    )
    wildlife_value: Optional[Literal["low", "moderate", "high"]] = Field(
        None, description="Value to local urban wildlife: pollinators, birds, small mammals"
    )
    fire_risk: Optional[Literal["low", "moderate", "high"]] = Field(
        None, description="Fire risk / flammability rating relevant to urban California"
    )
    tree_category: Literal["palm", "broadleaf", "spreading", "coniferous", "columnar", "ornamental", "default"] = Field(
        description=(
            "Visual silhouette category for map icon rendering. "
            "palm = fan fronds on tall slender trunk (palms). "
            "broadleaf = round canopy on short trunk (oaks, maples, most deciduous). "
            "spreading = wide flat canopy, broader than tall (plane trees, acacias). "
            "coniferous = triangular/spire shape (pines, cypress, firs). "
            "columnar = narrow upright oval, taller than wide (Italian cypress, columnar trees). "
            "ornamental = small flowering tree with visible blooms (cherry, plum, crabapple). "
            "default = generic round tree when shape is unclear."
        )
    )


# ── Icon drawing ───────────────────────────────────────────────────────────────

CATEGORY_COLORS: dict[str, tuple[int, int, int]] = {
    "palm":       (230, 168,  53),
    "broadleaf":  ( 76, 175,  80),
    "spreading":  (139, 195,  74),
    "coniferous": ( 46, 125,  50),
    "columnar":   ( 67, 160,  71),
    "ornamental": (233,  30,  99),
    "default":    (102, 187, 106),
}
TRUNK_COLOR = (93, 64, 55, 255)


def _c(rgb: tuple[int, int, int], a: int = 255) -> tuple[int, int, int, int]:
    return (*rgb, a)


def draw_tree_icon(category: str, size: int = ICON_SIZE) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    col = _c(CATEGORY_COLORS.get(category, CATEGORY_COLORS["default"]))
    cx = size / 2

    def trunk(top: float, width: float = 0.1) -> None:
        tw = size * width
        d.rectangle([cx - tw / 2, size * top, cx + tw / 2, size * 0.96], fill=TRUNK_COLOR)

    if category == "palm":
        trunk(0.35, 0.08)
        top_y = size * 0.35
        for angle_deg in range(-70, 71, 28):
            rad = math.radians(angle_deg - 90)
            length = size * 0.38
            ex = cx + math.cos(rad) * length
            ey = top_y + math.sin(rad) * length
            perp = rad + math.pi / 2
            w = size * 0.055
            d.polygon([
                (round(cx + math.cos(perp) * w), round(top_y + math.sin(perp) * w)),
                (round(cx - math.cos(perp) * w), round(top_y - math.sin(perp) * w)),
                (round(ex), round(ey)),
            ], fill=col)

    elif category == "broadleaf":
        trunk(0.55)
        r = size * 0.32
        cy = size * 0.38
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)

    elif category == "spreading":
        trunk(0.50)
        rx, ry = size * 0.42, size * 0.22
        cy = size * 0.38
        d.ellipse([cx - rx, cy - ry, cx + rx, cy + ry], fill=col)

    elif category == "coniferous":
        trunk(0.70, 0.08)
        d.polygon([
            (round(cx),                  round(size * 0.08)),
            (round(cx + size * 0.28),    round(size * 0.72)),
            (round(cx - size * 0.28),    round(size * 0.72)),
        ], fill=col)

    elif category == "columnar":
        trunk(0.60, 0.08)
        rx, ry = size * 0.20, size * 0.32
        cy = size * 0.38
        d.ellipse([cx - rx, cy - ry, cx + rx, cy + ry], fill=col)

    elif category == "ornamental":
        trunk(0.55, 0.08)
        r = size * 0.26
        cy = size * 0.40
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)
        bloom = (248, 187, 208, 220)
        dot_r = size * 0.055
        for ox, oy in [(-0.10, -0.08), (0.12, 0.04), (-0.04, 0.10), (0.08, -0.12)]:
            bx, by = cx + size * ox, cy + size * oy
            d.ellipse([bx - dot_r, by - dot_r, bx + dot_r, by + dot_r], fill=bloom)

    else:  # default
        trunk(0.55)
        r = size * 0.30
        cy = size * 0.38
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)

    # Soft glow outline using GaussianBlur on the alpha mask (replaces the
    # expensive pixel-walk approach; same visual result, GPU-friendly)
    mask = img.getchannel("A")
    glow_alpha = mask.filter(ImageFilter.GaussianBlur(1.8))
    glow = Image.new("RGBA", (size, size), (255, 255, 255, 0))
    glow.putalpha(glow_alpha)
    result = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    result.alpha_composite(glow)
    result.alpha_composite(img)
    return result


def to_raw_rgba_b64(img: Image.Image) -> str:
    """Encode image as raw RGBA bytes (no PNG header) → base64 string.
    Matches exactly what MapLibre addImage({ width, height, data: Uint8Array }) expects.
    """
    return base64.b64encode(img.tobytes()).decode()


# ── External data sources ───────────────────────────────────────────────────────

HEADERS = {"User-Agent": "sf-tree-enrichment/1.0 (github.com/sf-tree-reporting)"}


def fetch_wikipedia_text(scientific_name: str) -> str | None:
    slug = scientific_name.replace(" ", "_")

    # REST summary endpoint — concise intro paragraph, best for LLM context
    r = requests.get(
        f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}",
        headers=HEADERS,
        timeout=10,
    )
    if r.status_code == 200:
        data = r.json()
        extract = data.get("extract", "")
        if extract:
            return extract

    # MediaWiki API fallback — fuller intro section, handles redirects
    r = requests.get(
        "https://en.wikipedia.org/w/api.php",
        params={
            "action": "query",
            "prop": "extracts",
            "exintro": True,
            "titles": scientific_name,
            "format": "json",
            "redirects": 1,
        },
        headers=HEADERS,
        timeout=10,
    )
    if r.status_code == 200:
        pages = r.json().get("query", {}).get("pages", {})
        for page in pages.values():
            if page.get("pageid", -1) != -1:
                extract = page.get("extract", "")
                if extract:
                    return extract

    return None


def fetch_powo_text(scientific_name: str) -> str | None:
    """Fetch descriptive text from Plants of the World Online (POWO / Kew).

    POWO curates the World Checklist of Vascular Plants and provides
    habitat, morphology, and distribution descriptions.
    """
    try:
        r = requests.get(
            "https://powo.science.kew.org/api/2/search",
            params={"q": scientific_name, "f": "species_f"},
            headers=HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        results = r.json().get("results", [])
        if not results:
            return None
        fq_id = results[0].get("fqId")
        if not fq_id:
            return None

        r2 = requests.get(
            f"https://powo.science.kew.org/api/2/taxon/{fq_id}",
            params={"fields": "descriptions,distribution"},
            headers=HEADERS,
            timeout=10,
        )
        if r2.status_code != 200:
            return None
        data = r2.json()

        parts = []
        for desc_block in data.get("descriptions", []):
            for item in desc_block.get("descriptions", []):
                char = item.get("characteristic", "")
                text = item.get("description", "")
                if text:
                    parts.append(f"{char}: {text}" if char else text)

        # Include native distribution regions
        dist = data.get("distribution", {})
        natives = [
            r.get("name")
            for r in dist.get("natives", [])
            if r.get("name")
        ]
        if natives:
            parts.append(f"Native distribution: {', '.join(natives)}")

        return "\n".join(parts) if parts else None
    except Exception:
        return None


def fetch_wfo_text(scientific_name: str) -> str | None:
    """Fetch taxonomic context from World Flora Online (WFO).

    WFO provides stable WFO-IDs, accepted names, and family/order
    classification useful for resolving synonyms and taxonomy.
    """
    try:
        r = requests.get(
            "https://list.worldfloraonline.org/api.php",
            params={"terms": scientific_name, "fuzzy": "false", "full": "true"},
            headers=HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        docs = data.get("docs", [])
        if not docs:
            return None
        doc = docs[0]

        parts = []
        accepted = doc.get("acceptedNameScientificName") or doc.get("scientificName")
        if accepted:
            parts.append(f"Accepted scientific name: {accepted}")
        if doc.get("family"):
            parts.append(f"Family: {doc['family']}")
        if doc.get("order"):
            parts.append(f"Order: {doc['order']}")
        if doc.get("taxonRemarks"):
            parts.append(f"Notes: {doc['taxonRemarks']}")
        if doc.get("nativeDistribution"):
            parts.append(f"Native distribution: {doc['nativeDistribution']}")

        return "\n".join(parts) if parts else None
    except Exception:
        return None


# ── Enrichment ─────────────────────────────────────────────────────────────────

def enrich_species(q_species: str, client) -> TreeEnrichment | None:
    scientific_name = q_species.split("::")[0].strip()
    if not scientific_name:
        return None
    wiki_name = SYNONYMS.get(scientific_name.lower(), scientific_name)

    # Gather text from all available sources
    wiki_text  = fetch_wikipedia_text(wiki_name)
    powo_text  = fetch_powo_text(scientific_name)
    wfo_text   = fetch_wfo_text(scientific_name)

    if not wiki_text and not powo_text and not wfo_text:
        print(
            f"  [skip] no content found for {scientific_name!r} (lookup: {wiki_name!r})",
            file=sys.stderr,
        )
        return None

    sources_used = ", ".join(
        label for label, text in [("Wikipedia", wiki_text), ("POWO", powo_text), ("WFO", wfo_text)]
        if text
    )
    print(f"    [sources] {sources_used}", file=sys.stderr)

    context_parts = []
    if wiki_text:
        context_parts.append(f"Wikipedia:\n{wiki_text}")
    if powo_text:
        context_parts.append(f"Plants of the World Online (POWO / Kew):\n{powo_text}")
    if wfo_text:
        context_parts.append(f"World Flora Online (WFO):\n{wfo_text}")
    combined_text = "\n\n".join(context_parts)

    try:
        return client.chat.completions.create(
            response_model=TreeEnrichment,
            messages=[{
                "role": "user",
                "content": (
                    "You are enriching tree data for an urban forestry dataset in San Francisco, CA.\n"
                    "Extract structured information about this tree species from the reference text below.\n"
                    "Be conservative with numeric estimates — use None if the sources don't clearly state a value.\n\n"
                    f"Species: {scientific_name}\n\n"
                    f"Wikipedia lookup: {wiki_name}\n\n"
                    f"Reference text:\n{combined_text}"
                ),
            }],
        )
    except Exception as e:
        print(f"  [error] instructor failed for {scientific_name!r}: {e}", file=sys.stderr)
        return None


# ── Species list ───────────────────────────────────────────────────────────────

def get_all_species() -> list[str]:
    """Return all distinct species values from the tree dataset."""
    conn = duckdb.connect()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT species
            FROM read_parquet(?)
            WHERE plant_type = 'Tree'
              AND species IS NOT NULL
              AND lower(trim(species)) NOT IN ('::', 'tree', 'to be determine''d')
            ORDER BY species
            """,
            [TREE_INFO_PARQUET],
        ).fetchall()
        return [row[0] for row in rows if not should_skip_species(row[0])]
    finally:
        conn.close()


def get_already_enriched() -> tuple[set[str], set[str]]:
    """Return (all_enriched, complete_enriched).

    'complete' means every core Optional field has a non-null extracted value.
    Completeness is computed from existing column values so it works even before
    the is_complete column was added to the parquet.
    """
    conn = duckdb.connect()
    try:
        rows = conn.execute(
            """
            SELECT
              species,
              (common_names IS NOT NULL AND trim(common_names) != ''
               AND native_status IS NOT NULL
               AND is_evergreen IS NOT NULL
               AND mature_height_ft IS NOT NULL
               AND canopy_spread_ft IS NOT NULL
               AND growth_rate IS NOT NULL
               AND lifespan_years IS NOT NULL
               AND drought_tolerance IS NOT NULL
               AND bloom_season IS NOT NULL
               AND wildlife_value IS NOT NULL
               AND fire_risk IS NOT NULL
               AND tree_category IS NOT NULL) AS is_complete
            FROM read_parquet(?)
            WHERE species IS NOT NULL
            """,
            [ENRICHMENT_PARQUET],
        ).fetchall()
        all_enriched      = {row[0] for row in rows}
        complete_enriched = {row[0] for row in rows if row[1]}
        return all_enriched, complete_enriched
    except Exception:
        return set(), set()
    finally:
        conn.close()


def compute_is_complete(enrichment: TreeEnrichment) -> bool:
    """True only when every core field has a non-null extracted value."""
    return all([
        bool(enrichment.common_names),
        enrichment.is_evergreen      is not None,
        enrichment.mature_height_ft  is not None,
        enrichment.canopy_spread_ft  is not None,
        enrichment.growth_rate       is not None,
        enrichment.lifespan_years    is not None,
        enrichment.drought_tolerance is not None,
        enrichment.bloom_season      is not None,
        enrichment.wildlife_value    is not None,
        enrichment.fire_risk         is not None,
    ])


# ── Arrow table ────────────────────────────────────────────────────────────────

SCHEMA = pa.schema([
    ("species",          pa.string()),
    ("common_names",     pa.string()),       # comma-separated
    ("native_status",    pa.string()),
    ("is_evergreen",     pa.bool_()),
    ("mature_height_ft", pa.float32()),
    ("canopy_spread_ft", pa.float32()),
    ("growth_rate",      pa.string()),
    ("lifespan_years",   pa.string()),
    ("drought_tolerance",pa.string()),
    ("bloom_season",     pa.string()),
    ("wildlife_value",   pa.string()),
    ("fire_risk",        pa.string()),
    ("tree_category",    pa.string()),
    ("is_complete",      pa.bool_()),        # all core Optional fields are non-null
    ("icon_rgba_b64",    pa.string()),
    ("icon_width",       pa.int32()),
    ("icon_height",      pa.int32()),
    ("enriched_at",      pa.timestamp("us", tz="UTC")),
])


def build_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def emit(table: pa.Table) -> None:
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = instructor.from_provider(
        "google/gemini-2.5-pro",
        vertexai=True,
        project="preqldata",
        location="us-central1"      # e.g., us-central1, europe-west1
    )

    already_enriched, complete_enriched = get_already_enriched()
    all_species = get_all_species()
    # Process species that are new OR previously enriched but incomplete
    to_process = [s for s in all_species if s not in complete_enriched]
    incomplete_count = sum(1 for s in to_process if s in already_enriched)

    print(
        f"[info] {len(all_species)} total species | "
        f"{len(already_enriched)} already enriched "
        f"({len(complete_enriched)} complete, {incomplete_count} incomplete) | "
        f"{len(to_process)} to process",
        file=sys.stderr,
    )

    new_rows: list[dict] = []
    for q_species in to_process:
        status = "re-enrich" if q_species in already_enriched else "new"
        print(f"  [{status}] {q_species}", file=sys.stderr)
        enrichment = enrich_species(q_species, client)
        if enrichment is None:
            continue

        icon = draw_tree_icon(enrichment.tree_category)
        is_complete = compute_is_complete(enrichment)
        print(f"    [complete={is_complete}]", file=sys.stderr)
        new_rows.append({
            "species":          q_species,
            "common_names":     ", ".join(enrichment.common_names),
            "native_status":    enrichment.native_status,
            "is_evergreen":     enrichment.is_evergreen,
            "mature_height_ft": enrichment.mature_height_ft,
            "canopy_spread_ft": enrichment.canopy_spread_ft,
            "growth_rate":      enrichment.growth_rate,
            "lifespan_years":   enrichment.lifespan_years,
            "drought_tolerance":enrichment.drought_tolerance,
            "bloom_season":     enrichment.bloom_season,
            "wildlife_value":   enrichment.wildlife_value,
            "fire_risk":        enrichment.fire_risk,
            "tree_category":    enrichment.tree_category,
            "is_complete":      is_complete,
            "icon_rgba_b64":    to_raw_rgba_b64(icon),
            "icon_width":       ICON_SIZE,
            "icon_height":      ICON_SIZE,
            "enriched_at":      datetime.now(tz=timezone.utc),
        })

    # Merge with existing remote enrichment data and emit full merged output.
    # Trilogy persists the datasource output to configured storage.
    # Species we just processed replace their old rows (handles re-enrichment).
    re_processed = {row["species"] for row in new_rows}

    if already_enriched:
        conn = duckdb.connect()
        try:
            existing = conn.execute(
                """
                SELECT
                  species,
                  common_names,
                  native_status,
                  is_evergreen,
                  mature_height_ft,
                  canopy_spread_ft,
                  growth_rate,
                  lifespan_years,
                  drought_tolerance,
                  bloom_season,
                  wildlife_value,
                  fire_risk,
                  tree_category,
                  (common_names IS NOT NULL AND trim(common_names) != ''
                   AND native_status IS NOT NULL
                   AND is_evergreen IS NOT NULL
                   AND mature_height_ft IS NOT NULL
                   AND canopy_spread_ft IS NOT NULL
                   AND growth_rate IS NOT NULL
                   AND lifespan_years IS NOT NULL
                   AND drought_tolerance IS NOT NULL
                   AND bloom_season IS NOT NULL
                   AND wildlife_value IS NOT NULL
                   AND fire_risk IS NOT NULL
                   AND tree_category IS NOT NULL) AS is_complete,
                  icon_rgba_b64,
                  icon_width,
                  icon_height,
                  enriched_at
                FROM read_parquet(?)
                """,
                [ENRICHMENT_PARQUET],
            ).fetch_arrow_table()
        except Exception:
            existing = None
        finally:
            conn.close()

        if existing is not None:
            # Drop rows for species we just re-processed so new results replace them
            if re_processed:
                keep_mask = pc.invert(
                    pc.is_in(existing.column("species"), pa.array(sorted(re_processed)))
                )
                existing = existing.filter(keep_mask)

            # Normalize enriched_at timezone metadata if needed
            tz_idx = existing.schema.get_field_index("enriched_at")
            if tz_idx >= 0:
                target_ts = SCHEMA.field("enriched_at").type
                current_ts = existing.schema.field(tz_idx).type
                if current_ts != target_ts:
                    existing = existing.set_column(
                        tz_idx,
                        "enriched_at",
                        pc.cast(existing.column(tz_idx), target_ts, safe=False),
                    )

            merged = pa.concat_tables([existing, build_table(new_rows)])
        else:
            merged = build_table(new_rows)
    else:
        merged = build_table(new_rows)

    emit(merged)
