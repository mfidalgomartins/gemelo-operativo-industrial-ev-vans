# Arquitectura del Panel EV

## Artefacto oficial

`outputs/dashboard/industrial-ev-operating-command-center.html`

Fichero HTML estático con carga de datos embebida. Chart.js y las fuentes se cargan desde CDN, por lo que la visualización requiere acceso de red.

La plantilla se distribuye como recurso del paquete en `src/gemelo_operativo_ev/dashboard/dashboard.html`; `dashboard/renderer.py` valida sus marcadores y materializa los payloads. Esta separación permite mantener el generador Python sin modificar el diseño, los estilos ni los componentes existentes.

## Ruta de construcción

```bash
python -m gemelo_operativo_ev.run_pipeline       # canalización completa
python -m gemelo_operativo_ev.ev_build_dashboard # solo panel, si los CSVs ya existen
```

## Composición técnica

- **Librería de gráficos**: Chart.js 4.4.3, cargada desde CDN.
- **Tipografía**: Archivo (texto y cifras) + IBM Plex Mono (contextos tabulares: ejes, columnas numéricas, códigos), cargadas desde Google Fonts.
- **Espina de flujo**: recorrido físico del vehículo (producción → patio → carga → logística → expedición, con energía como soporte). Cada etapa muestra su medida característica y su Índice de Prioridad Operativa, y acota el panel a su área al seleccionarla.
- **Gráficos**: 19 canvas Chart.js — caudal, secuencia, patio, carga, expedición, escenarios, priorización OPI y diagnóstico EV/ICE.
- **Banda KPI**: 7 indicadores en primera vista, cada uno con su serie temporal y su estado frente al umbral publicado.
- **Filtros interactivos**: fecha, turno, propulsión, versión, área, zona patio, zona carga y severidad — aplicados en cliente sin llamadas al servidor.
- **Temas**: oscuro por defecto en la primera visita; el conmutador permite cambiar a claro y la elección queda persistida en `localStorage`, donde manda sobre el valor por defecto en las visitas siguientes. Cada modo tiene sus propios pasos de color, verificados contra su superficie; no es una inversión automática del otro.

## Principios de diseño

- **Los KPI del corte completo son los gobernados.** Sobre el período completo y sin filtros activos, la banda muestra los valores de `kpi_operativos.csv` sin recalcularlos: recalcularlos en cliente sobre datos ya agregados produce cifras distintas de las publicadas. En cuanto el contexto se acota, la única lectura posible es la recalculada, y el panel lo declara explícitamente.
- **El color de las marcas es semántico y fijo**, nunca decorativo: tinta para lo observado, pizarra discontinua para el plan u objetivo, cobalto para la dimensión EV, verdemar para energía y carga, y carmín para pérdida, riesgo y congestión. El carmín significa lo mismo en una marca que en un estado, así que un color de estado nunca suplanta a una serie.
- **Sin dobles ejes.** Dos escalas verticales en un mismo gráfico inventan una correlación que los datos no contienen; las medidas de magnitud distinta van en paneles separados.
- **El estado nunca viaja sólo en el color**: siempre lo acompaña su texto.
- **Toda serie tiene su tabla equivalente**, accesible desde el botón «Datos» de cada tarjeta.
- La paleta categórica está verificada con el validador de contraste y daltonismo en ambos modos (separación mínima ΔE 28,1 en claro y 20,0 en oscuro, frente al umbral objetivo de 12).
- Cero lógica de puntuación en el HTML; la puntuación ocurre en la canalización Python.
- JSON embebido en el HTML; las únicas llamadas de red son las dependencias visuales declaradas.
- Construcción validada con manifiesto técnico (`dashboard_build_manifest.json`) que verifica tamaño, presencia de contratos de diseño y ausencia de marcadores pendientes.

## Manifiesto de construcción

`outputs/reports/dashboard_build_manifest.json` registra:
- ruta oficial del artefacto
- tamaño en bytes
- comprobaciones estructurales (filtros, gráficos, temas, banda KPI)
