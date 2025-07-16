import tkinter as tk
import sys
from pathlib import Path

# Añade el directorio src al path de Python
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.controllers.convenios_controller import ConveniosController
from src.controllers.anticipos_controller import AnticiposController
from src.controllers.base_controller import BaseMensualController
from src.views.main_window import MainWindow

def main():
    try:
        root = tk.Tk()
        controller_convenios = ConveniosController(None)  # Pasamos None temporalmente
        controller_anticipos = AnticiposController(None)
        controller_base_mensual = BaseMensualController()# Placeholder para el controlador de anticipos si es necesario
        
        
        view= MainWindow(root, controller_convenios, controller_anticipos, controller_base_mensual)
        controller_anticipos.view = view
        controller_convenios.view = view  # Establecemos la vista después
        root.mainloop()
    except Exception as e:
        print(f"Error inesperado en la aplicación: {str(e)}")
    finally:
        print("Aplicación finalizada")

if __name__ == "__main__":
    main()