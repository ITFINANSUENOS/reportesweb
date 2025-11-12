# src/views/config_view/config_view.py

class AppConfig:
    title: str = "Procesador de Reportes Financieros"
    geometry: str = "900x500" # Un poco más de alto para que respire mejor
    
    # --- Paleta de Colores ---
    bg_color: str = "#f0f0f0"          # Color de fondo principal
    accent_color: str = "#4b6cb7"      # Color principal para botones y elementos activos
    secondary_color: str = "#2d3747"   # Color para hover o elementos secundarios
    text_color: str = "#333333"        # Color del texto general
    
    # --- NUEVO: Colores específicos para botones ---
    button_text_color: str = "#FFFFFF" # Texto blanco para que resalte en el botón
    button_pressed_color: str = "#182848" # Un azul más oscuro para el efecto de clic

    output_filename: str = "reporte_financiero.xlsx"