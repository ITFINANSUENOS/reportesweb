# src/views/config_view/style_assets.py
from PIL import Image, ImageDraw

def create_rounded_button_images(config, width=120, height=40, radius=10):
    """
    Genera y guarda las imágenes para los estados de un botón con esquinas redondeadas.
    Usa los colores definidos en el objeto de configuración.
    """
    
    def create_image(color):
        """Función interna para crear una imagen redondeada de un color específico."""
        image = Image.new("RGBA", (width, height), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle(
            ((0, 0), (width, height)), 
            fill=color, 
            radius=radius
        )
        return image

    # Crear imagen para el estado normal
    normal_img = create_image(config.accent_color)
    normal_img.save("button_normal.png")

    # Crear imagen para el estado 'hover' (cuando el mouse está encima)
    hover_img = create_image(config.secondary_color)
    hover_img.save("button_hover.png")

    # Crear imagen para el estado 'pressed' (al hacer clic)
    pressed_img = create_image(config.button_pressed_color)
    pressed_img.save("button_pressed.png")

    print("Imágenes de botones generadas exitosamente.")