#!/usr/bin/env python3
"""
Office Layout Generator
=======================
Generates potential layouts for a 12'7" x 8'5" dual-purpose office/guest room.
Uses Gemini for image generation and Gemini Flash for automatic grading.

Usage:
    export GEMINI_API_KEY="your-key-here"
    pip install flask google-genai
    python app.py

Then open http://localhost:5000 in your browser.
"""

import os
import json
import base64
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

# We lazily initialise the Gemini client so import-time errors are friendlier.
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
# Room specification
# ---------------------------------------------------------------------------
ROOM_SPEC = {
    "length": "12 feet 7 inches",  # longer walls (top & bottom)
    "width": "8 feet 5 inches",    # shorter walls (left & right)
    "window": "Centered on the longer wall (12'7\" wall, far side from door)",
    "door": "On the opposite longer wall, offset to one side",
    "required_furniture": ["desk (approx 48\"x24\")", "office chair", "sleeper sofa (approx 72\"x36\" closed, 72\"x52\" open)"],
    "style": "Cozy, homey, warm — serves as both a productive office and a welcoming guest bedroom",
    "flooring": "Warm hardwood or wood-look LVP",
    "notes": "When sofa is pulled out as a bed it must not block the door and there must be enough clearance to walk around."
}

# ---------------------------------------------------------------------------
# Predefined layout ideas
# ---------------------------------------------------------------------------
LAYOUTS = [
    {
        "id": "window-workspace",
        "name": "Window Workspace",
        "tagline": "Desk faces the light, sofa along the entry wall",
        "description": (
            "The desk (48\"x24\") is positioned against the window wall, centered under the window "
            "so you face natural light while working. The office chair sits in front of the desk "
            "facing the window. The sleeper sofa (72\" wide) runs along the opposite long wall "
            "(the door wall), placed to the left of the door so the door can still swing open freely. "
            "A small bookshelf or side table sits in the corner to the right of the sofa. "
            "A warm area rug anchors the center of the room. When the sofa pulls out, it extends "
            "toward the window wall — there's ~5 feet of clearance, enough for the bed plus walking space."
        ),
        "furniture_placement": {
            "desk": "Centered on 12'7\" window wall, facing window",
            "chair": "In front of desk, facing window",
            "sleeper_sofa": "Against opposite 12'7\" wall, left of door",
        }
    },
    {
        "id": "cozy-corner",
        "name": "Cozy Corner Nook",
        "tagline": "Desk tucked in a corner, sofa under the window for reading light",
        "description": (
            "The sleeper sofa (72\" wide) is positioned along the window wall, centered under the "
            "window — perfect for reading in natural light or gazing outside. The desk is tucked into "
            "the far corner (the corner of the window wall and the short wall away from the door), "
            "angled slightly so you can glance out the window. The office chair sits at the desk. "
            "A floor lamp stands next to the sofa. A woven basket and throw blanket on the sofa "
            "add warmth. When the sofa pulls out, it extends toward the door wall — the desk stays "
            "out of the way in its corner, and the door has room to open."
        ),
        "furniture_placement": {
            "desk": "In far corner of window wall and right short wall",
            "chair": "At desk, facing into room",
            "sleeper_sofa": "Centered under window on 12'7\" wall",
        }
    },
    {
        "id": "split-zones",
        "name": "Split Zone",
        "tagline": "Office zone by the window, guest zone near the door",
        "description": (
            "The room is divided into two zones. The office zone occupies the window side: the desk "
            "runs along the right short wall (8'5\"), with the chair facing the short wall and the "
            "window to the left for side-lighting. The guest zone is near the door: the sleeper sofa "
            "sits against the left short wall (opposite the desk wall), perpendicular to the door wall. "
            "This creates a natural corridor from the door into the room. A small console table behind "
            "the sofa acts as a divider. Warm pendant lighting and a rug define each zone."
        ),
        "furniture_placement": {
            "desk": "Along right 8'5\" short wall, near window wall",
            "chair": "At desk, facing right wall, window to left",
            "sleeper_sofa": "Along left 8'5\" short wall, near door wall",
        }
    },
    {
        "id": "diagonal-flow",
        "name": "Diagonal Flow",
        "tagline": "Furniture on opposing corners for maximum open space",
        "description": (
            "The desk is placed in the corner where the window wall meets the left short wall, "
            "positioned at a 45-degree angle across the corner. The chair faces into the room with "
            "the window behind and to the right. The sleeper sofa is placed against the door wall "
            "(the long wall opposite the window), pushed toward the right short wall — away from the "
            "door. This diagonal arrangement opens up the center of the room, creating a spacious "
            "feeling despite the small dimensions. A round area rug in the center and a small side "
            "table next to the sofa complete the look. The sofa pulls out toward the window wall "
            "with comfortable clearance."
        ),
        "furniture_placement": {
            "desk": "Corner-angled where window wall meets left short wall",
            "chair": "At desk, facing into room diagonally",
            "sleeper_sofa": "Against door wall, toward right short wall, away from door",
        }
    },
    {
        "id": "murphy-style",
        "name": "Library Lounge",
        "tagline": "Sofa as the centerpiece, desk along the side wall",
        "description": (
            "The sleeper sofa is the focal point, placed against the right short wall (8'5\"), "
            "facing into the room. Flanking it are a floor lamp and a small side table. The desk "
            "runs along the door wall (12'7\" long wall), positioned to the right of the door so "
            "there's ample space for the door to swing. The chair is at the desk with back to the "
            "door wall. Floating shelves above the desk hold books and plants. The window wall is "
            "kept mostly clear with just a tall plant in the corner, letting maximum light flood in. "
            "When the sofa pulls out, it extends toward the left short wall — plenty of room."
        ),
        "furniture_placement": {
            "desk": "Along door wall (12'7\" wall), right of door",
            "chair": "At desk, back to door wall, facing window",
            "sleeper_sofa": "Against right 8'5\" short wall, facing into room",
        }
    },
]

# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _room_context() -> str:
    return (
        "Room dimensions: 12 feet 7 inches long × 8 feet 5 inches wide. "
        "There is a window centered on one of the 12'7\" long walls. "
        "A door is on the opposite 12'7\" long wall. "
        "The room must contain exactly: one desk (approx 48\"×24\"), one office chair, "
        "and one sleeper sofa (approx 72\"×36\" when closed). "
        "Style: cozy, homey, warm. Dual-purpose office and guest room. "
        "Warm wood floors, soft textiles, plants, warm lighting. "
        "The room should feel inviting and lived-in, not sterile."
    )


def build_image_prompt(layout: dict, feedback: str | None = None) -> str:
    """Build the prompt sent to the image generation model."""
    prompt_parts = [
        "Generate a photorealistic interior design rendering of a small room viewed from a slightly elevated angle (bird's eye perspective tilted ~30°) showing the full floor plan.",
        "",
        f"ROOM: {_room_context()}",
        "",
        f"LAYOUT NAME: {layout['name']}",
        f"LAYOUT DESCRIPTION: {layout['description']}",
        "",
        "STYLE DIRECTION: Cozy home office that doubles as a guest room. "
        "Think warm wood tones, a soft area rug, a throw blanket draped on the sofa, "
        "a desk lamp with warm light, a small potted plant, perhaps fairy lights or "
        "a pendant lamp. Natural light streams through the window. "
        "The vibe is Scandinavian-hygge meets mid-century warmth.",
        "",
        "IMPORTANT CONSTRAINTS:",
        "- Show the room from above at an angle so ALL furniture is visible and spatial relationships are clear.",
        "- The window must be on one long wall and the door on the opposite long wall.",
        "- All three pieces of furniture (desk, office chair, sleeper sofa) must be clearly visible.",
        "- The room proportions must look like a narrow rectangle (roughly 3:2 ratio).",
        "- Maintain realistic furniture sizes relative to room dimensions.",
        "- DO NOT add extra furniture beyond what is described.",
    ]
    if feedback:
        prompt_parts.extend([
            "",
            f"USER FEEDBACK — incorporate this into the new rendering: {feedback}",
        ])
    return "\n".join(prompt_parts)


def build_grade_prompt(layout: dict) -> str:
    """Build the prompt sent to Gemini Flash for grading."""
    return (
        "You are an interior design floor-plan reviewer. Analyze this generated room image "
        "against the following specification and provide a JSON grade.\n\n"
        f"SPECIFICATION:\n{_room_context()}\n\n"
        f"EXPECTED LAYOUT:\n{layout['description']}\n\n"
        "FURNITURE PLACEMENT:\n"
        + "\n".join(f"  - {k}: {v}" for k, v in layout["furniture_placement"].items())
        + "\n\n"
        "Grade the image on these criteria (each 1-10):\n"
        "1. room_proportions — Does the room look like a 12'7\" × 8'5\" rectangle?\n"
        "2. furniture_presence — Are all three items (desk, chair, sleeper sofa) clearly present?\n"
        "3. furniture_placement — Does placement match the layout description?\n"
        "4. window_door — Is the window on one long wall and door on the opposite?\n"
        "5. style_cozy — Does it feel cozy, homey, and warm?\n"
        "6. realism — Is the rendering photorealistic and well-composed?\n\n"
        "Return ONLY a JSON object with these keys plus an 'overall' average score (1-10) "
        "and a 'feedback' string with 1-2 sentences of constructive criticism.\n"
        "Example: {\"room_proportions\": 8, \"furniture_presence\": 9, ...., \"overall\": 7.5, "
        "\"feedback\": \"The sofa looks too small relative to the room.\"}\n"
        "Return ONLY the JSON, no markdown fences."
    )


# ---------------------------------------------------------------------------
# Gemini API calls
# ---------------------------------------------------------------------------

def generate_image(layout: dict, feedback: str | None = None) -> tuple[str, str]:
    """
    Generate a layout image using Gemini's image generation.
    Returns (image_filename, image_generation_model_used).
    """
    from google.genai import types

    client = get_client()
    prompt = build_image_prompt(layout, feedback)
    image_id = f"{layout['id']}_{uuid.uuid4().hex[:8]}"

    # Try gemini-2.0-flash-preview-image-generation (native image gen)
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
        # Walk through parts looking for inline_data images
        for part in response.candidates[0].content.parts:
            if part.inline_data and part.inline_data.mime_type.startswith("image/"):
                ext = part.inline_data.mime_type.split("/")[-1]
                if ext == "jpeg":
                    ext = "jpg"
                filename = f"{image_id}.{ext}"
                filepath = IMAGE_DIR / filename
                filepath.write_bytes(part.inline_data.data)
                return filename, model_name

        # If no image part found, raise to fall through
        raise ValueError("No image part in response")

    except Exception as e:
        print(f"[WARN] {model_name} failed: {e}")

    # Fallback: try imagen-3.0-generate-002
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
            img = response.generated_images[0]
            filename = f"{image_id}.png"
            filepath = IMAGE_DIR / filename
            filepath.write_bytes(img.image.image_bytes)
            return filename, model_name
    except Exception as e:
        print(f"[WARN] {model_name} failed: {e}")

    raise RuntimeError(
        "All image generation models failed. Check your GEMINI_API_KEY and quota."
    )


def grade_image(image_filename: str, layout: dict) -> dict:
    """
    Use Gemini Flash to grade the generated image against the floor plan spec.
    Returns a dict of scores.
    """
    from google.genai import types

    client = get_client()
    filepath = IMAGE_DIR / image_filename
    image_bytes = filepath.read_bytes()
    mime = "image/png" if image_filename.endswith(".png") else "image/jpeg"

    prompt = build_grade_prompt(layout)

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[
            types.Part.from_bytes(data=image_bytes, mime_type=mime),
            prompt,
        ],
    )
    text = response.text.strip()
    # Strip markdown fences if model adds them anyway
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"overall": 0, "feedback": f"Could not parse grading response: {text[:200]}"}


# ---------------------------------------------------------------------------
# In-memory store for generated results
# ---------------------------------------------------------------------------
# Each entry: {id, layout_id, filename, model, grade, feedback_used, timestamp}
generated: list[dict] = []


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", layouts=LAYOUTS)


@app.route("/api/layouts")
def api_layouts():
    return jsonify(LAYOUTS)


@app.route("/api/generate", methods=["POST"])
def api_generate():
    data = request.json or {}
    layout_id = data.get("layout_id")
    feedback = data.get("feedback", "").strip() or None

    layout = next((l for l in LAYOUTS if l["id"] == layout_id), None)
    if not layout:
        return jsonify({"error": f"Unknown layout: {layout_id}"}), 400

    try:
        filename, model_used = generate_image(layout, feedback)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Grade it
    try:
        grade = grade_image(filename, layout)
    except Exception as e:
        grade = {"overall": 0, "feedback": f"Grading failed: {e}"}

    entry = {
        "id": uuid.uuid4().hex[:12],
        "layout_id": layout_id,
        "layout_name": layout["name"],
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
    print(f"  Room: {ROOM_SPEC['length']} × {ROOM_SPEC['width']}")
    print(f"  Layouts available: {len(LAYOUTS)}")
    print("=" * 60)
    if not GEMINI_API_KEY:
        print("\n⚠  WARNING: GEMINI_API_KEY not set!")
        print("   Set it:  export GEMINI_API_KEY='your-key'")
        print("   Get one: https://aistudio.google.com/apikey\n")
    print("  Starting server at http://localhost:5000\n")
    app.run(debug=True, port=5000)
