from PIL import Image, ImageDraw, ImageTk
import io

def create_rounded_button_images(config, width=120, height=40, radius=10):
    """
    Genera imágenes en memoria (sin guardarlas en disco)
    para los estados del botón con esquinas redondeadas.
    Retorna un diccionario con las imágenes listas para usar en Tkinter.
    """

    def create_image(color):
        """Crea una imagen redondeada en memoria."""
        image = Image.new("RGBA", (width, height), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle(
            ((0, 0), (width, height)),
            fill=color,
            radius=radius
        )
        return image

    # Crear las imágenes (solo en memoria)
    normal_img = create_image(config.accent_color)
    hover_img = create_image(config.secondary_color)
    pressed_img = create_image(config.button_pressed_color)

    # Convertirlas a PhotoImage (para Tkinter)
    normal_photo = ImageTk.PhotoImage(normal_img)
    hover_photo = ImageTk.PhotoImage(hover_img)
    pressed_photo = ImageTk.PhotoImage(pressed_img)

    print("✅ Imágenes de botones generadas en memoria (no se guardan en disco).")

    # Retornar un diccionario con todas
    return {
        "normal": normal_photo,
        "hover": hover_photo,
        "pressed": pressed_photo
    }