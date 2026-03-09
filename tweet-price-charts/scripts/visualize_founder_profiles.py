#!/usr/bin/env python3
"""
Create a visual summary of founder content profiles.
"""

import json
from pathlib import Path
from datetime import datetime

def create_ascii_chart(data, max_width=50):
    """Create an ASCII bar chart."""
    if not data:
        return []

    max_val = max(data.values()) if data else 1
    lines = []
    for label, value in sorted(data.items(), key=lambda x: -x[1]):
        bar_width = int((value / max_val) * max_width) if max_val > 0 else 0
        bar = "█" * bar_width
        lines.append(f"  {label:15s} {bar} {value:.1f}%")
    return lines

def main():
    print("=" * 100)
    print("FOUNDER CONTENT PROFILE COMPARISON")
    print("=" * 100)

    # Define founder data based on analysis
    founders = {
        "a1lon9 (PUMP)": {
            "tweets": 102,
            "style": "Casual Memecoin Builder",
            "length": 135,
            "caps": 37.3,
            "emojis": 2.0,
            "top_categories": {
                "other": 48.0,
                "media": 28.4,
                "engagement": 12.7,
                "hype": 7.8,
                "metrics": 7.8,
            }
        },
        "chameleon_jeff (HYPE)": {
            "tweets": 34,
            "style": "Technical Debunker",
            "length": 199,
            "caps": 55.9,
            "emojis": 0.0,
            "top_categories": {
                "media": 38.2,
                "metrics": 32.4,
                "other": 23.5,
                "defensive": 14.7,
                "shipping": 14.7,
            }
        },
        "cz_binance (ASTER)": {
            "tweets": 306,
            "style": "Media Machine",
            "length": 139,
            "caps": 62.1,
            "emojis": 47.1,
            "top_categories": {
                "media": 79.4,
                "other": 16.0,
                "engagement": 12.7,
                "metrics": 5.2,
                "shipping": 4.2,
            }
        },
        "pasternak (BELIEVE)": {
            "tweets": 81,
            "style": "Builder's Journal",
            "length": 173,
            "caps": 25.9,
            "emojis": 0.0,
            "top_categories": {
                "other": 38.3,
                "shipping": 35.8,
                "media": 33.3,
                "hype": 12.3,
                "engagement": 12.3,
            }
        },
        "weremeow (JUP)": {
            "tweets": 662,
            "style": "Long-Form Explainer",
            "length": 297,
            "caps": 37.0,
            "emojis": 13.3,
            "top_categories": {
                "other": 41.8,
                "media": 35.6,
                "shipping": 14.0,
                "engagement": 14.0,
                "hype": 10.0,
            }
        },
        "keoneHD (MONAD)": {
            "tweets": 60,
            "style": "Excited Sharer",
            "length": 143,
            "caps": 36.7,
            "emojis": 3.3,
            "top_categories": {
                "media": 53.3,
                "shipping": 25.0,
                "other": 20.0,
                "engagement": 15.0,
                "hype": 10.0,
            }
        },
        "theunipcs (USELESS)": {
            "tweets": 420,
            "style": "CAPS-LOCK Marketer",
            "length": 503,
            "caps": 98.3,
            "emojis": 21.2,
            "top_categories": {
                "media": 36.7,
                "metrics": 32.1,
                "other": 30.0,
                "engagement": 20.0,
                "hype": 9.5,
            }
        },
    }

    # Print individual profiles
    for founder, data in founders.items():
        print(f"\n{'─' * 100}")
        print(f"FOUNDER: {founder:40s} Style: {data['style']}")
        print(f"Sample: {data['tweets']} tweets")
        print(f"{'─' * 100}")
        print(f"Tweet Length: {data['length']:.0f} chars | CAPS: {data['caps']:.1f}% | Emojis: {data['emojis']:.1f}%")
        print()
        print("Content Distribution:")
        for line in create_ascii_chart(data['top_categories'], 40):
            print(line)

    # Comparative analysis
    print(f"\n{'=' * 100}")
    print("COMPARATIVE METRICS")
    print(f"{'=' * 100}")

    # Tweet length comparison
    print("\nTWEET LENGTH (characters):")
    length_data = {founder.split()[0]: data['length'] for founder, data in founders.items()}
    for line in create_ascii_chart(length_data, 60):
        print(line)

    # CAPS usage comparison
    print("\nCAPS USAGE (%):")
    caps_data = {founder.split()[0]: data['caps'] for founder, data in founders.items()}
    for line in create_ascii_chart(caps_data, 60):
        print(line)

    # Emoji usage comparison
    print("\nEMOJI USAGE (%):")
    emoji_data = {founder.split()[0]: data['emojis'] for founder, data in founders.items()}
    for line in create_ascii_chart(emoji_data, 60):
        print(line)

    # Content focus comparison
    print("\n" + "=" * 100)
    print("CONTENT FOCUS COMPARISON")
    print("=" * 100)

    # Shipping focus
    print("\nSHIPPING FOCUS (%):")
    shipping = {}
    for founder, data in founders.items():
        shipping[founder.split()[0]] = data['top_categories'].get('shipping', 0)
    for line in create_ascii_chart(shipping, 60):
        print(line)

    # Defensive content
    print("\nDEFENSIVE CONTENT (%):")
    defensive = {}
    for founder, data in founders.items():
        defensive[founder.split()[0]] = data['top_categories'].get('defensive', 0)
    for line in create_ascii_chart(defensive, 60):
        print(line)

    # Engagement seeking
    print("\nENGAGEMENT SEEKING (%):")
    engagement = {}
    for founder, data in founders.items():
        engagement[founder.split()[0]] = data['top_categories'].get('engagement', 0)
    for line in create_ascii_chart(engagement, 60):
        print(line)

    # Media sharing
    print("\nMEDIA SHARING (%):")
    media = {}
    for founder, data in founders.items():
        media[founder.split()[0]] = data['top_categories'].get('media', 0)
    for line in create_ascii_chart(media, 60):
        print(line)

    # Summary archetypes
    print("\n" + "=" * 100)
    print("FOUNDER ARCHETYPES")
    print("=" * 100)

    archetypes = [
        ("a1lon9", "The Casual Memecoin Builder", "Short, CAPS-heavy, minimal shipping talk"),
        ("chameleon_jeff", "The Technical Debunker", "Long, data-driven, constantly fights FUD"),
        ("cz_binance", "The Media Machine", "79% links, heavy emojis, minimal text"),
        ("pasternak", "The Builder's Journal", "35% shipping content, professional, no emojis"),
        ("weremeow", "The Long-Form Explainer", "297 char avg, detailed threads, evolved to curator"),
        ("keoneHD", "The Excited Sharer", "Early-stage building, 53% media, active shipping"),
        ("theunipcs", "The CAPS-LOCK Marketer", "503 char tweets, 98% CAPS, extreme engagement-seeking"),
    ]

    for founder, archetype, description in archetypes:
        print(f"\n{founder:20s} → {archetype:30s}")
        print(f"{'':20s}   {description}")

    print("\n" + "=" * 100)
    print("ANOMALY DETECTION QUICK REFERENCE")
    print("=" * 100)

    anomalies = [
        ("chameleon_jeff", "Emoji usage, tweet <100 chars, pure hype without data"),
        ("cz_binance", "No link in tweet, no emojis, long technical thread"),
        ("theunipcs", "No CAPS, tweet <200 chars, serious/defensive tone"),
        ("pasternak", "Emoji usage, heavy CAPS, defensive rant"),
        ("weremeow", "Tweet <100 chars, pure hype without explanation"),
        ("a1lon9", "Long defensive thread, detailed technical content"),
        ("keoneHD", "Defensive content, very long threads, heavy metrics"),
    ]

    for founder, flags in anomalies:
        print(f"\n{founder:20s} → {flags}")

    print("\n" + "=" * 100)

if __name__ == "__main__":
    main()
