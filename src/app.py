import tkinter as tk
import sys
from pathlib import Path

# Añade el directorio src al path de Python
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.controllers.convenios_controller import ConveniosController
from src.controllers.anticipos_controller import AnticiposController
from src.controllers.base_controller import BaseMensualController
from src.controllers.datacredito_controller import DataCreditoController
from src.controllers.cifin_contoller import CifinController
from src.views.main_window import MainWindow
from src.controllers.novedades_controller import NovedadesAnalisisController
from src.controllers.ecollect_controller import EcollectController

def main():
    try:
        root = tk.Tk()
        controller_convenios = ConveniosController(None)
        controller_anticipos = AnticiposController(None)
        controller_base_mensual = BaseMensualController()
        controller_datacredito = DataCreditoController()
        controller_cifin = CifinController()
        controller_novedades_analisis = NovedadesAnalisisController()
        controller_ecollect = EcollectController()

        main_view = MainWindow(
            root, 
            controller_convenios, 
            controller_anticipos,
            controller_base_mensual,
            controller_datacredito,
            controller_cifin,
            controller_novedades_analisis,
            controller_ecollect  
        )

        controller_anticipos.set_view(main_view)
        controller_convenios.set_view(main_view)

        root.mainloop()
    except Exception as e:
        print(f"Error inesperado en la aplicación: {str(e)}")
    finally:
        print("Aplicación finalizada")

if __name__ == "__main__":
    main()