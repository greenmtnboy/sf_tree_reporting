#!/usr/bin/env python3
"""
Office Layout Generator
=======================
Generates 20 potential layouts for a 12'7" x 8'5" dual-purpose office/guest room.
Each layout includes labeled furniture coordinates, creative storage solutions,
and multiple camera perspectives for image generation.

Uses Gemini for image generation and Gemini Flash for automatic floor-plan grading.

Usage:
    export GEMINI_API_KEY="your-key-here"
    pip install flask google-genai
    python app.py

Then open http://localhost:5000 in your browser.
"""

import os
import json
import uuid
import time
import re
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
IMAGE_DIR = Path(__file__).parent / "static" / "images"
IMAGE_DIR.mkdir(parents=True, exist_ok=True)

_client = None


def get_client():
    global _client
    if _client is None:
        if not GEMINI_API_KEY:
            raise RuntimeError(
                "Set GEMINI_API_KEY env var before running. "
                "Get one at https://aistudio.google.com/apikey"
            )
        from google import genai
        _client = genai.Client(api_key=GEMINI_API_KEY)
    return _client


# ---------------------------------------------------------------------------
# Room constants (inches)
# ---------------------------------------------------------------------------
ROOM_W = 151   # 12'7" — long walls (top=window, bottom=door)
ROOM_D = 101   # 8'5"  — short walls (left & right)

# Window: centered on top long wall, ~60" wide
WINDOW = {"x": 46, "y": 0, "w": 60}
# Door: bottom long wall, offset left, 30" wide, hinge at left side
DOOR = {"x": 20, "y": ROOM_D, "w": 30}


# ---------------------------------------------------------------------------
# Helpers for defining furniture
# ---------------------------------------------------------------------------
def _f(fid, label, ftype, x, y, w, h):
    """Shorthand for a furniture item."""
    return {"id": fid, "label": label, "type": ftype, "x": x, "y": y, "w": w, "h": h}


# ---------------------------------------------------------------------------
# Camera perspectives
# ---------------------------------------------------------------------------
PERSPECTIVES = [
    {
        "id": "elevated-corner",
        "name": "Elevated Corner",
        "prompt": (
            "Viewed from an elevated position near the top-left corner of the room "
            "(near where the window wall meets the left short wall), looking diagonally "
            "across and down toward the opposite corner. Camera is at roughly 7 feet high, "
            "tilted ~40° downward. This shows the full room layout with depth."
        ),
    },
    {
        "id": "from-doorway",
        "name": "From Doorway",
        "prompt": (
            "Viewed from the doorway at eye level (~5'6\" high), standing in the door frame "
            "and looking straight into the room toward the window on the far wall. "
            "Natural light from the window is visible ahead. This is the first impression "
            "a guest would see entering the room."
        ),
    },
    {
        "id": "from-window",
        "name": "From Window",
        "prompt": (
            "Viewed from the window wall at eye level (~5'6\" high), as if standing with "
            "your back to the window looking toward the door. Natural light streams in "
            "from behind the viewer, illuminating the room. The door is visible on the "
            "far wall."
        ),
    },
    {
        "id": "overhead",
        "name": "Overhead Plan",
        "prompt": (
            "Viewed directly from above, looking straight down at the room — a true "
            "architectural bird's-eye floor plan view. Camera at ceiling height. "
            "All furniture positions and spatial relationships are clearly visible. "
            "The room reads as a rectangle with the window on the top long wall "
            "and door on the bottom long wall."
        ),
    },
]


# ---------------------------------------------------------------------------
# 20 Layouts
# ---------------------------------------------------------------------------
# Room coordinate system:
#   Origin (0,0) = top-left corner
#   X runs along long walls (0..151"), Y runs along short walls (0..101")
#   Top wall (y=0): 12'7" WINDOW wall
#   Bottom wall (y=101): 12'7" DOOR wall
#   Left wall (x=0): 8'5" short wall
#   Right wall (x=151): 8'5" short wall
#
# Furniture dimensions:
#   Desk: 48"×24"   Chair: 22"×22"   Sleeper sofa: 72"×36" (closed)
# ---------------------------------------------------------------------------

LAYOUTS = [
    # ------------------------------------------------------------------
    # 1. Window Workspace
    # ------------------------------------------------------------------
    {
        "id": "window-workspace",
        "name": "Window Workspace",
        "tagline": "Desk faces the light, sofa along the entry wall",
        "description": (
            "The desk is centered under the window so you face natural light while "
            "working. The office chair sits behind the desk. The sleeper sofa runs "
            "along the door wall to the right of the door. A floating shelf above "
            "the sofa holds books and a trailing plant. A small filing cabinet "
            "sits to the right of the desk. Warm area rug in the center."
        ),
        "storage": ["Floating shelf above sofa (52\")", "Small filing cabinet beside desk"],
        "furniture": [
            _f("desk",    "Desk 48\"×24\"",          "desk",    51, 1,  48, 24),
            _f("chair",   "Office Chair",            "chair",   64, 27, 22, 22),
            _f("sofa",    "Sleeper Sofa 72\"×36\"",  "sofa",    58, 64, 72, 36),
            _f("shelf1",  "Floating Shelf 52\"",     "shelf",   68, 60, 52, 4),
            _f("cabinet", "Filing Cabinet",          "storage", 101, 1, 15, 20),
        ],
    },
    # ------------------------------------------------------------------
    # 2. Cozy Corner Nook
    # ------------------------------------------------------------------
    {
        "id": "cozy-corner",
        "name": "Cozy Corner Nook",
        "tagline": "Sofa under the window for reading light, desk in far corner",
        "description": (
            "The sleeper sofa is centered under the window — perfect for reading "
            "in natural light. The desk is in the far-right corner where the window "
            "wall meets the right short wall, turned so you can glance out the window. "
            "A tall corner bookshelf tower fills the left corner of the window wall. "
            "A small side table with a lamp sits beside the sofa."
        ),
        "storage": ["Corner bookshelf tower (left corner)", "Side table with drawer"],
        "furniture": [
            _f("sofa",      "Sleeper Sofa 72\"×36\"",  "sofa",    40, 1,  72, 36),
            _f("desk",      "Desk 48\"×24\"",          "desk",    126, 5, 24, 48),
            _f("chair",     "Office Chair",            "chair",   102, 20, 22, 22),
            _f("bookshelf", "Corner Bookshelf Tower",  "storage", 0,  1,  16, 16),
            _f("sidetbl",   "Side Table + Lamp",       "storage", 35, 5,  14, 14),
        ],
    },
    # ------------------------------------------------------------------
    # 3. Split Zone
    # ------------------------------------------------------------------
    {
        "id": "split-zones",
        "name": "Split Zone",
        "tagline": "Office zone by window, guest zone near door",
        "description": (
            "The room is split into two distinct zones. Office zone (window side): "
            "desk runs along the right short wall near the window, chair faces the "
            "wall with the window to the left for side-light. Guest zone (door side): "
            "sleeper sofa sits along the left short wall. A narrow console table in "
            "the middle acts as a room divider with storage baskets underneath. "
            "A wall-mounted shelf above the desk holds supplies."
        ),
        "storage": ["Console table with baskets (room divider)", "Wall shelf above desk"],
        "furniture": [
            _f("desk",    "Desk 48\"×24\"",          "desk",    126, 5,  24, 48),
            _f("chair",   "Office Chair",            "chair",   102, 20, 22, 22),
            _f("sofa",    "Sleeper Sofa 72\"×36\"",  "sofa",    0,  5,  36, 72),
            _f("console", "Console Table + Baskets", "storage", 50, 40, 48, 14),
            _f("shelf1",  "Wall Shelf",              "shelf",   133, 1, 16, 4),
        ],
    },
    # ------------------------------------------------------------------
    # 4. Diagonal Flow
    # ------------------------------------------------------------------
    {
        "id": "diagonal-flow",
        "name": "Diagonal Flow",
        "tagline": "Corner-angled desk opens up the center",
        "description": (
            "The desk is angled at 45° in the corner where the window wall meets "
            "the left short wall. The chair faces diagonally into the room. The "
            "sleeper sofa is along the door wall, pushed toward the right short wall "
            "away from the door. A round side table sits beside the sofa. A wall-mounted "
            "shelf above the sofa and a pegboard above the desk keep things organized. "
            "The diagonal arrangement maximizes open floor space in the center."
        ),
        "storage": ["Wall shelf above sofa", "Pegboard above desk corner"],
        "furniture": [
            _f("desk",     "Desk 48\"×24\" (angled)", "desk",    2,  2,  40, 40),
            _f("chair",    "Office Chair",            "chair",   30, 30, 22, 22),
            _f("sofa",     "Sleeper Sofa 72\"×36\"",  "sofa",    60, 64, 72, 36),
            _f("sidetbl",  "Round Side Table",        "storage", 134, 68, 14, 14),
            _f("shelf1",   "Wall Shelf",              "shelf",   70, 60, 40, 4),
            _f("pegboard", "Pegboard",                "storage", 2,  1,  30, 3),
        ],
    },
    # ------------------------------------------------------------------
    # 5. Library Lounge
    # ------------------------------------------------------------------
    {
        "id": "library-lounge",
        "name": "Library Lounge",
        "tagline": "Sofa as centerpiece, desk along door wall with shelves above",
        "description": (
            "The sleeper sofa is the focal point against the right short wall, "
            "facing into the room. The desk runs along the door wall to the right "
            "of the door. Two rows of floating shelves above the desk hold books "
            "and plants. A floor lamp flanks the sofa. The window wall is kept "
            "clear for maximum light."
        ),
        "storage": ["Two floating shelves above desk", "Floor lamp with shelf base"],
        "furniture": [
            _f("sofa",   "Sleeper Sofa 72\"×36\"",  "sofa",  114, 14, 36, 72),
            _f("desk",   "Desk 48\"×24\"",          "desk",  55,  76, 48, 24),
            _f("chair",  "Office Chair",            "chair", 72,  52, 22, 22),
            _f("shelf1", "Floating Shelf (upper)",  "shelf", 58,  68, 42, 4),
            _f("shelf2", "Floating Shelf (lower)",  "shelf", 58,  73, 42, 4),
            _f("lamp",   "Floor Lamp",              "storage", 110, 14, 6, 6),
        ],
    },
    # ------------------------------------------------------------------
    # 6. L-Desk Haven
    # ------------------------------------------------------------------
    {
        "id": "l-desk-haven",
        "name": "L-Desk Haven",
        "tagline": "L-shaped desk wraps the corner for maximum work surface",
        "description": (
            "An L-shaped desk arrangement wraps the corner where the window wall "
            "meets the right short wall — the main surface runs along the window "
            "wall and a return extends down the right wall. The chair sits at the "
            "L's inner corner. The sofa is against the left short wall. A rolling "
            "file cabinet tucks under the desk return. A small shelf above the desk "
            "holds supplies."
        ),
        "storage": ["Rolling file cabinet under desk return", "Wall shelf above desk"],
        "furniture": [
            _f("desk-main",   "Desk Surface 54\"×24\"",   "desk",    96,  1,  54, 24),
            _f("desk-return", "Desk Return 24\"×36\"",    "desk",    126, 25, 24, 36),
            _f("chair",       "Office Chair",             "chair",   104, 27, 22, 22),
            _f("sofa",        "Sleeper Sofa 72\"×36\"",   "sofa",    0,  28, 36, 72),
            _f("filecab",     "Rolling File Cabinet",     "storage", 128, 45, 18, 14),
            _f("shelf1",      "Wall Shelf",               "shelf",   100, 1,  48, 4),
        ],
    },
    # ------------------------------------------------------------------
    # 7. The Gallery
    # ------------------------------------------------------------------
    {
        "id": "the-gallery",
        "name": "The Gallery",
        "tagline": "Desk floats mid-room facing the window like an easel",
        "description": (
            "The desk floats in the middle of the room facing the window — like "
            "a creative studio. The chair is behind it facing the window. The sofa "
            "runs along the left short wall. An art-ledge shelf runs the full length "
            "of the right wall for displaying prints and photos. A small bookcase "
            "sits in the upper-left corner. The open arrangement feels airy despite "
            "the small room."
        ),
        "storage": ["Art ledge shelf (full right wall)", "Small bookcase in corner"],
        "furniture": [
            _f("desk",      "Desk 48\"×24\"",         "desk",    51, 32, 48, 24),
            _f("chair",     "Office Chair",            "chair",   64, 58, 22, 22),
            _f("sofa",      "Sleeper Sofa 72\"×36\"",  "sofa",    0,  14, 36, 72),
            _f("artledge",  "Art Ledge Shelf",         "shelf",   148, 10, 3, 80),
            _f("bookcase",  "Small Bookcase",          "storage", 0,  0,  20, 12),
        ],
    },
    # ------------------------------------------------------------------
    # 8. Scandinavian Minimal
    # ------------------------------------------------------------------
    {
        "id": "scandi-minimal",
        "name": "Scandinavian Minimal",
        "tagline": "Wall-mounted floating desk, everything airy and light",
        "description": (
            "A slim wall-mounted floating desk hangs under the window — no legs, "
            "so the floor feels open. The chair is a clean-lined Scandinavian style. "
            "The sofa sits along the door wall to the right of the door. A single "
            "long shelf runs above the desk. Wall-mounted hooks on the left wall "
            "hold bags and a mirror. Absolutely minimal — only what's needed."
        ),
        "storage": ["Long floating shelf above desk", "Wall hooks on left wall"],
        "furniture": [
            _f("desk",   "Floating Desk 40\"×20\"",   "desk",  56, 1,  40, 20),
            _f("chair",  "Office Chair",               "chair", 64, 23, 22, 22),
            _f("sofa",   "Sleeper Sofa 72\"×36\"",     "sofa",  55, 64, 72, 36),
            _f("shelf1", "Floating Shelf 48\"",        "shelf", 52, 1,  48, 3),
            _f("hooks",  "Wall Hooks + Mirror",        "storage", 0, 30, 4, 30),
        ],
    },
    # ------------------------------------------------------------------
    # 9. The Alcove
    # ------------------------------------------------------------------
    {
        "id": "the-alcove",
        "name": "The Alcove",
        "tagline": "Sofa creates a cozy reading alcove by the window",
        "description": (
            "The sleeper sofa is pushed into the left side of the window wall, "
            "creating a cozy reading alcove. A curtain rod spans the room at the "
            "sofa's edge to let you close off the sleeping area. The desk is against "
            "the right short wall. A corner shelf unit sits beside the sofa. "
            "Fairy lights drape along the alcove ceiling for warmth."
        ),
        "storage": ["Corner shelf unit by sofa", "Curtain divider rod"],
        "furniture": [
            _f("sofa",    "Sleeper Sofa 72\"×36\"",  "sofa",    0,  1,  72, 36),
            _f("desk",    "Desk 48\"×24\"",          "desk",    126, 30, 24, 48),
            _f("chair",   "Office Chair",            "chair",   102, 42, 22, 22),
            _f("shelf1",  "Corner Shelf Unit",       "storage", 0,  38, 16, 24),
            _f("curtain", "Curtain Rod",             "storage", 74, 0,  2, 101),
        ],
    },
    # ------------------------------------------------------------------
    # 10. Bookworm's Retreat
    # ------------------------------------------------------------------
    {
        "id": "bookworm-retreat",
        "name": "Bookworm's Retreat",
        "tagline": "Floor-to-ceiling bookshelf dominates one wall",
        "description": (
            "A floor-to-ceiling bookshelf spans the entire left short wall — "
            "maximum book storage. The desk sits beside it, facing the window "
            "wall with side-light from the left. The sofa is along the door wall "
            "to the right. A small reading lamp and plant sit on the desk. "
            "A rolling library ladder leans against the bookshelf (decorative in "
            "this small room, but charming)."
        ),
        "storage": ["Full-wall bookshelf (left wall, floor to ceiling)", "Desk drawer unit"],
        "furniture": [
            _f("bookshelf", "Floor-to-Ceiling Bookshelf", "storage", 0,  0, 12, 101),
            _f("desk",      "Desk 48\"×24\"",             "desk",    14, 5, 48, 24),
            _f("chair",     "Office Chair",                "chair",   30, 31, 22, 22),
            _f("sofa",      "Sleeper Sofa 72\"×36\"",      "sofa",    78, 64, 72, 36),
            _f("drawers",   "Under-Desk Drawer Unit",      "storage", 14, 5, 16, 22),
        ],
    },
    # ------------------------------------------------------------------
    # 11. Cottage Office
    # ------------------------------------------------------------------
    {
        "id": "cottage-office",
        "name": "Cottage Office",
        "tagline": "Vintage charm with woven baskets and warm wood",
        "description": (
            "The desk sits under the window, slightly off-center to the left. "
            "The sofa runs along the right short wall. Woven storage baskets "
            "tuck under the desk for files and supplies. A wooden crate serves "
            "as a side table beside the sofa. A small floating shelf with plants "
            "hangs above the sofa. Vintage-cottage aesthetic with lots of texture."
        ),
        "storage": ["Woven baskets under desk (×3)", "Wooden crate side table", "Floating shelf above sofa"],
        "furniture": [
            _f("desk",    "Desk 48\"×24\"",          "desk",    35, 1,  48, 24),
            _f("chair",   "Office Chair",            "chair",   52, 27, 22, 22),
            _f("sofa",    "Sleeper Sofa 72\"×36\"",  "sofa",    114, 14, 36, 72),
            _f("basket1", "Woven Basket",            "storage", 38, 2,  12, 10),
            _f("basket2", "Woven Basket",            "storage", 52, 2,  12, 10),
            _f("basket3", "Woven Basket",            "storage", 66, 2,  12, 10),
            _f("crate",   "Wooden Crate Table",      "storage", 110, 50, 14, 14),
            _f("shelf1",  "Floating Shelf",          "shelf",   118, 10, 28, 4),
        ],
    },
    # ------------------------------------------------------------------
    # 12. Mid-Century Modern
    # ------------------------------------------------------------------
    {
        "id": "midcentury",
        "name": "Mid-Century Modern",
        "tagline": "Credenza behind the sofa, clean lines throughout",
        "description": (
            "The desk sits against the left short wall, near the window for "
            "side-light. The chair faces the left wall. The sofa is centered "
            "in the room, facing the desk, with a credenza/sideboard behind it "
            "acting as both storage and a visual divider. The credenza holds "
            "books, a record player, and decorative objects. Tapered legs on "
            "everything. Warm walnut tones."
        ),
        "storage": ["Credenza/sideboard behind sofa (60\")", "Desk has single drawer"],
        "furniture": [
            _f("desk",      "Desk 48\"×24\"",          "desk",    2,  14, 24, 48),
            _f("chair",     "Office Chair",            "chair",   28, 28, 22, 22),
            _f("sofa",      "Sleeper Sofa 72\"×36\"",  "sofa",    40, 28, 72, 36),
            _f("credenza",  "Credenza 60\"×16\"",      "storage", 46, 66, 60, 16),
        ],
    },
    # ------------------------------------------------------------------
    # 13. The Nesting Office
    # ------------------------------------------------------------------
    {
        "id": "nesting-office",
        "name": "The Nesting Office",
        "tagline": "Console desk hides behind the sofa back",
        "description": (
            "The sofa is centered under the window. A narrow console-style desk "
            "sits directly behind the sofa back — you sit at the desk with the "
            "sofa behind you, facing into the room. Two ladder shelves flank the "
            "window in the upper corners. This layout feels like a living room "
            "that secretly has a workspace tucked in."
        ),
        "storage": ["Two ladder shelves flanking window", "Console desk has slim drawer"],
        "furniture": [
            _f("sofa",     "Sleeper Sofa 72\"×36\"",  "sofa",    40, 1,  72, 36),
            _f("desk",     "Console Desk 60\"×16\"",  "desk",    46, 39, 60, 16),
            _f("chair",    "Office Chair",            "chair",   68, 57, 22, 22),
            _f("ladder-l", "Ladder Shelf (L)",        "storage", 2,  1,  18, 12),
            _f("ladder-r", "Ladder Shelf (R)",        "storage", 131, 1, 18, 12),
        ],
    },
    # ------------------------------------------------------------------
    # 14. Standing Desk Flow
    # ------------------------------------------------------------------
    {
        "id": "standing-flow",
        "name": "Standing Desk Flow",
        "tagline": "Standing desk by the wall, sofa under the window",
        "description": (
            "A standing-height desk runs along the right short wall. A tall "
            "stool (not a full office chair) sits at the desk. The sofa is "
            "under the window for natural light. A rolling storage cart sits "
            "beside the desk for supplies and acts as a portable surface. "
            "A small floating shelf near the door holds keys and mail."
        ),
        "storage": ["Rolling storage cart", "Floating shelf by door"],
        "furniture": [
            _f("desk",  "Standing Desk 48\"×24\"",   "desk",    126, 10, 24, 48),
            _f("stool", "Tall Stool",                "chair",   104, 24, 18, 18),
            _f("sofa",  "Sleeper Sofa 72\"×36\"",    "sofa",    40, 1,  72, 36),
            _f("cart",  "Rolling Cart 18\"×14\"",    "storage", 126, 60, 18, 24),
            _f("shelf", "Entry Shelf",               "shelf",   55, 97, 30, 3),
        ],
    },
    # ------------------------------------------------------------------
    # 15. Zen Workspace
    # ------------------------------------------------------------------
    {
        "id": "zen-workspace",
        "name": "Zen Workspace",
        "tagline": "Low-profile everything, calm and grounded",
        "description": (
            "The desk sits centered under the window. The chair is a low-profile "
            "ergonomic model. The sofa is a low-slung modern sleeper against the "
            "right short wall. A low floating shelf runs along the left wall for "
            "minimal storage. A floor cushion in the corner provides extra seating. "
            "Everything is close to the ground — the room feels expansive. "
            "A single trailing plant hangs from a wall hook."
        ),
        "storage": ["Low floating shelf (left wall)", "Floor cushion storage nook"],
        "furniture": [
            _f("desk",    "Desk 48\"×24\"",          "desk",    51, 1,  48, 24),
            _f("chair",   "Low Office Chair",        "chair",   64, 27, 22, 22),
            _f("sofa",    "Low Sleeper Sofa 72\"×36\"", "sofa", 114, 14, 36, 72),
            _f("shelf1",  "Low Shelf (left wall)",   "shelf",   0,  50, 4, 40),
            _f("cushion", "Floor Cushion",           "storage", 4,  72, 22, 22),
        ],
    },
    # ------------------------------------------------------------------
    # 16. The Pocket Office
    # ------------------------------------------------------------------
    {
        "id": "pocket-office",
        "name": "The Pocket Office",
        "tagline": "Desk hides beside the door, sofa owns the window",
        "description": (
            "The sofa takes pride of place under the window. The desk is on "
            "the door wall, to the right of the door where it's tucked away "
            "but still has a view of the window. A fold-down wall shelf on the "
            "left wall provides extra workspace when needed. A wall-mounted "
            "cabinet above the desk keeps supplies tidy."
        ),
        "storage": ["Fold-down wall shelf (left wall)", "Wall-mounted cabinet above desk"],
        "furniture": [
            _f("sofa",    "Sleeper Sofa 72\"×36\"",   "sofa",    40, 1,  72, 36),
            _f("desk",    "Desk 48\"×24\"",           "desk",    58, 76, 48, 24),
            _f("chair",   "Office Chair",             "chair",   75, 52, 22, 22),
            _f("foldshf", "Fold-Down Wall Shelf",     "storage", 0,  40, 3, 30),
            _f("wallcab", "Wall Cabinet",             "shelf",   62, 72, 40, 4),
        ],
    },
    # ------------------------------------------------------------------
    # 17. Maximalist Storage
    # ------------------------------------------------------------------
    {
        "id": "maximalist",
        "name": "Maximalist Storage",
        "tagline": "Every surface stores something — organized chaos",
        "description": (
            "The desk is against the left short wall with a pegboard above it "
            "for tools, notes, and supplies. The sofa is against the right short "
            "wall with under-sofa storage bins. Floating shelves line the window "
            "wall flanking both sides of the window. An over-door organizer hangs "
            "on the back of the door. A small bookcase sits mid-room against the "
            "door wall. Every vertical inch is used."
        ),
        "storage": [
            "Pegboard above desk (36\"×24\")",
            "Under-sofa storage bins (×3)",
            "Floating shelves flanking window (×2)",
            "Over-door organizer",
            "Small bookcase on door wall",
        ],
        "furniture": [
            _f("desk",     "Desk 48\"×24\"",          "desk",    2,  14, 24, 48),
            _f("chair",    "Office Chair",            "chair",   28, 28, 22, 22),
            _f("sofa",     "Sleeper Sofa 72\"×36\"",  "sofa",    114, 14, 36, 72),
            _f("pegboard", "Pegboard 36\"×24\"",      "storage", 0,  8,  4, 36),
            _f("bin1",     "Storage Bin",             "storage", 118, 18, 28, 8),
            _f("bin2",     "Storage Bin",             "storage", 118, 28, 28, 8),
            _f("bin3",     "Storage Bin",             "storage", 118, 38, 28, 8),
            _f("shelf-l",  "Floating Shelf (L)",      "shelf",   5,  1,  36, 4),
            _f("shelf-r",  "Floating Shelf (R)",      "shelf",   110, 1, 36, 4),
            _f("bookcase", "Small Bookcase",          "storage", 58, 76, 24, 24),
            _f("overdoor", "Over-Door Organizer",     "storage", 24, 95, 22, 5),
        ],
    },
    # ------------------------------------------------------------------
    # 18. Window Daybed
    # ------------------------------------------------------------------
    {
        "id": "window-daybed",
        "name": "Window Daybed",
        "tagline": "Sofa as daybed under the window, shelves flanking like wings",
        "description": (
            "The sleeper sofa doubles as a daybed, centered under the window "
            "with throw pillows piled against one arm. Matching narrow bookshelves "
            "flank the window on each side like wings. The desk is on the door wall "
            "to the right of the door. The chair faces the window with the desk "
            "behind it. Airy and symmetrical."
        ),
        "storage": ["Narrow bookshelf left of window (14\"×30\")", "Narrow bookshelf right of window (14\"×30\")"],
        "furniture": [
            _f("sofa",     "Daybed / Sleeper Sofa 72\"×36\"", "sofa", 40, 1, 72, 36),
            _f("desk",     "Desk 48\"×24\"",           "desk",    58, 76, 48, 24),
            _f("chair",    "Office Chair",             "chair",   75, 52, 22, 22),
            _f("shelf-l",  "Bookshelf (left)",         "storage", 4,  1,  14, 30),
            _f("shelf-r",  "Bookshelf (right)",        "storage", 133, 1, 14, 30),
        ],
    },
    # ------------------------------------------------------------------
    # 19. The Peninsula
    # ------------------------------------------------------------------
    {
        "id": "the-peninsula",
        "name": "The Peninsula",
        "tagline": "Desk juts out from the wall as a room divider",
        "description": (
            "The desk is perpendicular to the right short wall, extending into "
            "the room like a peninsula and subtly dividing the space. The chair "
            "sits at the open end, facing the right wall. The sofa is against the "
            "left short wall. A narrow shelf above the desk on the right wall "
            "holds books. A storage bin tucks under the desk on the wall side."
        ),
        "storage": ["Wall shelf above desk (right wall)", "Under-desk storage bin"],
        "furniture": [
            _f("desk",    "Desk 48\"×24\" (peninsula)", "desk",  126, 28, 24, 48),
            _f("chair",   "Office Chair",               "chair", 102, 42, 22, 22),
            _f("sofa",    "Sleeper Sofa 72\"×36\"",     "sofa",  0,  14, 36, 72),
            _f("shelf1",  "Wall Shelf",                 "shelf", 140, 5,  10, 24),
            _f("bin",     "Under-Desk Bin",             "storage", 130, 30, 16, 12),
        ],
    },
    # ------------------------------------------------------------------
    # 20. Hygge Hideaway
    # ------------------------------------------------------------------
    {
        "id": "hygge-hideaway",
        "name": "Hygge Hideaway",
        "tagline": "Maximum cozy — textiles, warm light, and a storage ottoman",
        "description": (
            "The sofa is along the window wall on the left side, draped in "
            "a chunky knit throw. The desk is against the right short wall. "
            "A storage ottoman in the center serves as a footrest, extra seat, "
            "and hidden storage for blankets. A small bookcase sits between the "
            "sofa and the left wall. String lights trace the ceiling perimeter. "
            "This room begs you to stay."
        ),
        "storage": ["Storage ottoman (22\"×22\")", "Small bookcase beside sofa"],
        "furniture": [
            _f("sofa",     "Sleeper Sofa 72\"×36\"",  "sofa",    2,  1,  72, 36),
            _f("desk",     "Desk 48\"×24\"",          "desk",    126, 30, 24, 48),
            _f("chair",    "Office Chair",            "chair",   102, 42, 22, 22),
            _f("ottoman",  "Storage Ottoman",         "storage", 55, 45, 22, 22),
            _f("bookcase", "Small Bookcase",          "storage", 0,  38, 16, 28),
        ],
    },
]


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _room_context() -> str:
    return (
        "Room: 12 feet 7 inches long × 8 feet 5 inches wide (151\" × 101\"). "
        "Window centered on one 12'7\" long wall (top of plan). "
        "Door on opposite 12'7\" long wall (bottom of plan), offset left. "
        "Required furniture: one desk (~48\"×24\"), one office chair, "
        "one sleeper sofa (~72\"×36\" closed). "
        "Style: cozy, homey, warm. Dual-purpose office + guest room. "
        "Warm wood floors, soft textiles, plants, warm lighting."
    )


def _furniture_description(layout: dict) -> str:
    """Build a precise textual description of furniture placement from coordinates."""
    lines = []
    for f in layout["furniture"]:
        line = (
            f"- {f['label']}: top-left at ({f['x']}\", {f['y']}\") from room origin, "
            f"size {f['w']}\"×{f['h']}\" on the plan"
        )
        lines.append(line)
    return "\n".join(lines)


def build_image_prompt(
    layout: dict,
    perspective: dict,
    feedback: str | None = None,
) -> str:
    """Build the prompt sent to the image generation model."""
    storage_text = "\n".join(f"  - {s}" for s in layout.get("storage", []))
    prompt_parts = [
        "Generate a photorealistic interior design rendering of a small room.",
        "",
        f"ROOM: {_room_context()}",
        "",
        f"LAYOUT: \"{layout['name']}\" — {layout['description']}",
        "",
        f"FURNITURE COORDINATES (room is {ROOM_W}\"×{ROOM_D}\", origin=top-left, "
        f"window on top wall, door on bottom wall):",
        _furniture_description(layout),
        "",
        f"STORAGE SOLUTIONS:\n{storage_text}",
        "",
        f"CAMERA / PERSPECTIVE: {perspective['name']}\n{perspective['prompt']}",
        "",
        "STYLE DIRECTION: Cozy home office + guest room. "
        "Warm wood tones, soft area rug, throw blanket on sofa, "
        "desk lamp with warm light, small potted plants, pendant lamp or fairy lights. "
        "Natural light through the window. Scandinavian-hygge meets mid-century warmth.",
        "",
        "IMPORTANT CONSTRAINTS:",
        "- Follow the camera perspective exactly as described above.",
        "- The window MUST be on one long wall and the door on the OPPOSITE long wall.",
        "- All main furniture (desk, chair, sleeper sofa) MUST be clearly visible.",
        "- Room proportions must be a ~3:2 rectangle (12'7\" × 8'5\").",
        "- Maintain realistic furniture sizes relative to room dimensions.",
        "- Include the storage items described. Do NOT add extra large furniture.",
        "- Label nothing in the image — this should look like a real photo/rendering.",
    ]
    if feedback:
        prompt_parts.extend([
            "",
            f"USER FEEDBACK — incorporate this into the new rendering: {feedback}",
        ])
    return "\n".join(prompt_parts)


def build_grade_prompt(layout: dict, perspective: dict) -> str:
    """Build the prompt sent to Gemini Flash for grading."""
    return (
        "You are an interior design floor-plan reviewer. Analyze this generated room image "
        "against the specification and return a JSON grade.\n\n"
        f"SPECIFICATION:\n{_room_context()}\n\n"
        f"EXPECTED LAYOUT:\n{layout['description']}\n\n"
        f"FURNITURE COORDINATES:\n{_furniture_description(layout)}\n\n"
        f"CAMERA USED: {perspective['name']} — {perspective['prompt']}\n\n"
        "STORAGE EXPECTED:\n"
        + "\n".join(f"  - {s}" for s in layout.get("storage", []))
        + "\n\n"
        "Grade the image on these criteria (each 1-10):\n"
        "1. room_proportions — Does the room look like a 12'7\" × 8'5\" rectangle?\n"
        "2. furniture_presence — Are all three main items (desk, chair, sleeper sofa) present?\n"
        "3. furniture_placement — Does placement match the described coordinates?\n"
        "4. storage_presence — Are the storage items visible and correctly placed?\n"
        "5. window_door — Is the window on one long wall and door on the opposite?\n"
        "6. camera_angle — Does the perspective match what was requested?\n"
        "7. style_cozy — Does it feel cozy, homey, and warm?\n"
        "8. realism — Is the rendering photorealistic and well-composed?\n\n"
        "Return ONLY a JSON object with these keys, plus an 'overall' score (1-10, average), "
        "and a 'feedback' string with 1-2 sentences of constructive criticism.\n"
        "Return ONLY the JSON, no markdown fences."
    )


# ---------------------------------------------------------------------------
# Gemini API calls
# ---------------------------------------------------------------------------

def generate_image(
    layout: dict,
    perspective: dict,
    feedback: str | None = None,
) -> tuple[str, str]:
    """
    Generate a layout image via Gemini image generation.
    Returns (image_filename, model_used).
    """
    from google.genai import types

    client = get_client()
    prompt = build_image_prompt(layout, perspective, feedback)
    image_id = f"{layout['id']}_{perspective['id']}_{uuid.uuid4().hex[:8]}"

    # Try gemini-2.0-flash-preview-image-generation
    model_name = "gemini-2.0-flash-preview-image-generation"
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_modalities=["TEXT", "IMAGE"],
                temperature=1.0,
            ),
        )
        for part in response.candidates[0].content.parts:
            if part.inline_data and part.inline_data.mime_type.startswith("image/"):
                ext = part.inline_data.mime_type.split("/")[-1]
                if ext == "jpeg":
                    ext = "jpg"
                filename = f"{image_id}.{ext}"
                (IMAGE_DIR / filename).write_bytes(part.inline_data.data)
                return filename, model_name
        raise ValueError("No image part in response")
    except Exception as e:
        print(f"[WARN] {model_name} failed: {e}")

    # Fallback: imagen-3.0-generate-002
    model_name = "imagen-3.0-generate-002"
    try:
        response = client.models.generate_images(
            model=model_name,
            prompt=prompt,
            config=types.GenerateImagesConfig(
                number_of_images=1,
                output_mime_type="image/png",
            ),
        )
        if response.generated_images:
            filename = f"{image_id}.png"
            (IMAGE_DIR / filename).write_bytes(
                response.generated_images[0].image.image_bytes
            )
            return filename, model_name
    except Exception as e:
        print(f"[WARN] {model_name} failed: {e}")

    raise RuntimeError("All image generation models failed. Check GEMINI_API_KEY and quota.")


def grade_image(image_filename: str, layout: dict, perspective: dict) -> dict:
    """Use Gemini Flash to grade the image against the floor plan spec."""
    from google.genai import types

    client = get_client()
    image_bytes = (IMAGE_DIR / image_filename).read_bytes()
    mime = "image/png" if image_filename.endswith(".png") else "image/jpeg"

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[
            types.Part.from_bytes(data=image_bytes, mime_type=mime),
            build_grade_prompt(layout, perspective),
        ],
    )
    text = response.text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"overall": 0, "feedback": f"Could not parse grading response: {text[:200]}"}


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
generated: list[dict] = []

# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/layouts")
def api_layouts():
    return jsonify(LAYOUTS)


@app.route("/api/perspectives")
def api_perspectives():
    return jsonify(PERSPECTIVES)


@app.route("/api/generate", methods=["POST"])
def api_generate():
    data = request.json or {}
    layout_id = data.get("layout_id")
    perspective_id = data.get("perspective_id", "elevated-corner")
    feedback = data.get("feedback", "").strip() or None

    layout = next((l for l in LAYOUTS if l["id"] == layout_id), None)
    if not layout:
        return jsonify({"error": f"Unknown layout: {layout_id}"}), 400

    perspective = next((p for p in PERSPECTIVES if p["id"] == perspective_id), None)
    if not perspective:
        return jsonify({"error": f"Unknown perspective: {perspective_id}"}), 400

    try:
        filename, model_used = generate_image(layout, perspective, feedback)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        grade = grade_image(filename, layout, perspective)
    except Exception as e:
        grade = {"overall": 0, "feedback": f"Grading failed: {e}"}

    entry = {
        "id": uuid.uuid4().hex[:12],
        "layout_id": layout_id,
        "layout_name": layout["name"],
        "perspective_id": perspective_id,
        "perspective_name": perspective["name"],
        "filename": filename,
        "model": model_used,
        "grade": grade,
        "feedback_used": feedback,
        "timestamp": time.time(),
    }
    generated.append(entry)
    return jsonify(entry)


@app.route("/api/history")
def api_history():
    return jsonify(list(reversed(generated)))


@app.route("/images/<path:filename>")
def serve_image(filename):
    return send_from_directory(IMAGE_DIR, filename)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  Office Layout Generator")
    print(f"  Room: 12'7\" × 8'5\"  |  Layouts: {len(LAYOUTS)}  |  Perspectives: {len(PERSPECTIVES)}")
    print("=" * 60)
    if not GEMINI_API_KEY:
        print("\n⚠  WARNING: GEMINI_API_KEY not set!")
        print("   export GEMINI_API_KEY='your-key'")
        print("   https://aistudio.google.com/apikey\n")
    print("  http://localhost:5000\n")
    app.run(debug=True, port=5000)
