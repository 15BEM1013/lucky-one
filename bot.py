
    elif last_phase == "🟠 Bearish Indecision":
        return "⚪ Sideways (Bearish Bias)", "Low"

    # ==========================
    # SIDEWAYS
    # ==========================

    elif last_phase == "⚪ Sideways (Bullish Bias)":
        return "🟢 Bullish Momentum Building", "High"

    elif last_phase == "⚪ Sideways (Bearish Bias)":
        return "🔴 Bearish Momentum Building", "High"

    elif last_phase == "🟣 Market Compression":
        return "⚡ Strong Breakout Expected", "Medium"

    elif last_phase == "⚪ Transition / Indecision":
        return "Waiting for Confirmation", "Low"

    # ==========================
    # BULLISH
    # ==========================

    elif last_phase == "🟢 Bullish Recovery":
        return "🟢 Bullish Momentum Building", "High"

    elif last_phase == "🟢 Bullish Momentum Building":
        return "🚀 Bullish Breakout", "High"

    elif last_phase == "🚀 Bullish Breakout":
        return "🟢 Bullish Trend Continuation", "High"

    elif last_phase == "🟡 Bullish Momentum Fading":
        return "⚪ Sideways", "Medium"

    elif last_phase == "🟠 Bullish Pullback":
        return "🟢 Bullish Momentum Building", "Medium"

    elif last_phase == "🟡 Bullish Indecision":
        return "⚪ Sideways (Bullish Bias)", "Low"