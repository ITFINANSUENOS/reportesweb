
import time
import os
from src.models.base_config import configuracion, archivos_a_procesar, ORDEN_COLUMNAS_FINAL
from src.services.base_services.base import ReportService

def main():
 
    start_time = time.time()

    # 1. Instanciar el servicio, pasándole la configuración.
    service = ReportService(config=configuracion)

     # 2. Llamar al método principal del servicio, pasándole ahora el orden de las columnas
    reporte_final = service.generate_consolidated_report(
        file_paths=archivos_a_procesar,
        orden_columnas=ORDEN_COLUMNAS_FINAL 
    )

    # 3. Manejar el resultado final
    if reporte_final is not None and not reporte_final.empty:
        print("\n--- 📊 Vista Previa del Reporte Final ---")
        print(reporte_final.head())
        print(f"\n--- Total de registros: {len(reporte_final)} ---")
        
        # --- CAMBIO AQUÍ: Ya no se ordenan alfabéticamente ---
        # Ahora se muestran en el orden personalizado que definiste.
        columnas_finales = reporte_final.columns.tolist() 
        print(f"\n--- Columnas finales en el reporte ({len(columnas_finales)}) ---")
        print(columnas_finales)

        nombre_archivo_salida = 'Reporte_Consolidado_Final.xlsx'
        try:
            reporte_final.to_excel(nombre_archivo_salida, index=False, sheet_name='Reporte Consolidado')
            print(f"\n✨ ¡Éxito! El reporte final se ha guardado como '{nombre_archivo_salida}' ✨")
        except Exception as e:
            print(f"❌ Error al guardar el archivo de Excel: {e}")
    else:
        print("\n🛑 No se generó el reporte final debido a errores o falta de datos base.")
    
    end_time = time.time()
    print(f"\n⏳ Proceso completado en {end_time - start_time:.2f} segundos.")


if __name__ == "__main__":
    main()