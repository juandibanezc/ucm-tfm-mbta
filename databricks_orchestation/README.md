# Databricks Orchestration - TFM Big Data

<div align="center">

[![Powered by Databricks](https://img.shields.io/badge/Powered%20by-Databricks-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)

</div>

Este directorio contiene la configuración de orquestación para el proyecto **TFM Big Data** usando Databricks Asset Bundles (DAB) y el Databricks CLI.

## 📋 Tabla de Contenido

- [Estructura del Directorio](#estructura-del-directorio)
- [Descripción de Componentes](#descripción-de-componentes)
- [Requisitos Previos](#requisitos-previos)
- [Comandos de Materialización](#comandos-de-materialización)
- [Flujo de Materialización Recomendado](#flujo-de-materialización-recomendado)
- [Variables y Customización](#variables-y-customización)
- [Monitoreo y Logs](#monitoreo-y-logs)
- [Solución de Problemas](#solución-de-problemas)
- [Referencias](#referencias)

## Estructura del Directorio

```
databricks_orchestation/
├── databricks_jobs/
│   ├── databricks.yml          # Configuración principal del bundle
│   └── resources/
│       ├── tfm_current_load.job.yml    # Job de carga incremental
│       └── tfm_full_load.job.yml       # Job de carga completa
└── notebooks/
    └── Setup notebook.ipynb    # Notebook para setup inicial
```

## Descripción de Componentes

### 1. `databricks.yml`
Archivo de configuración principal del Databricks Asset Bundle que define:
- **Bundle Name**: `tfm_bigdata`
- **Variables**: 
  - `catalog`: Catálogo a utilizar (ej: `tfmlakehouse`)
  - `schema`: Esquema a utilizar (ej: `dev` o `prod`)
- **Targets**: Dos entornos configurados
  - **dev**: Modo development con prefijo `[dev my_user_name]` en recursos
  - **prod**: Modo production en ruta `/Workspace/Users/juandaib@ucm.es/.bundle/`

### 2. Jobs YAML

#### `tfm_current_load.job.yml`
Job de carga **incremental/diaria** que ejecuta los pipelines en secuencia:
- **landing** → **bronze** → **silver** → **gold**
- Cluster: `Standard_D4ds_v4`, single-node, PHOTON runtime
- Spark 16.4.x
- **Programación**: Cron `0 0 4 * * ?` (Diariamente a las 4:00 AM)
- Usa bibliotecas:
  - `processing_datalake-0.1-py3-none-any.whl`
  - Paquetes Spark: Hadoop Azure, Delta, Azure Storage

#### `tfm_full_load.job.yml`
Job de carga **completa** que ejecuta pipelines de primer cargamiento:
- **landing** → **bronze_full_load** → **silver_full_load** → **gold_full_load**
- Mismo cluster y configuración que current_load
- **Sin programación**: Se ejecuta bajo demanda

### 3. Setup Notebook
`Setup notebook.ipynb` realiza:
1. Selecciona catálogo `tfmlakehouse`
2. Crea esquemas (bronze, silver, gold)
3. Crea tablas Delta desde ubicaciones en Azure Storage

## Requisitos Previos

### 1. Instalación de Databricks CLI
```bash
# Instalar Databricks CLI
pip install databricks-cli
# O versión más reciente
pip install databricks
```

### 2. Autenticación en Databricks
```bash
# Configurar credenciales de Databricks
databricks configure --token
# Ingresar host (ej: https://adb-7405612198535189.9.azuredatabricks.net)
# Ingresar token de autenticación
```

Alternativamente, usando archivo `~/.databrickscfg`:
```ini
[DEFAULT]
host = https://adb-7405612198535189.9.azuredatabricks.net
token = dapi...
```

### 3. Requisitos Adicionales
- Acceso a Databricks workspace
- Credenciales en Databricks Secrets:
  - `secrets/kedro-secrets/azure-account-key`
  - `secrets/kedro-secrets/mbta_api_key`
  - `secrets/kedro-secrets/azure-account-name`
- Artefactos subidos a workspace:
  - `processing_datalake-0.1-py3-none-any.whl`
  - Archivo de configuración comprimido

## Comandos de Materialización

### 1. Inicializar el Bundle
```bash
cd databricks_orchestation/databricks_jobs

# Validar configuración
databricks bundle validate

# Ver el plan de despliegue (preview)
databricks bundle plan
```

### 2. Desplegar en Desarrollo (dev)
```bash
# Desplegar en target dev (default)
databricks bundle deploy

# O explícitamente:
databricks bundle deploy --target dev
```

**Resultado**: Los jobs se crean con prefijo `[dev username]` y los schedules están pausados.

### 3. Desplegar en Producción (prod)
```bash
# Desplegar en target prod
databricks bundle deploy --target prod
```

**Resultado**: Los jobs se crean sin prefijo, en producción, con schedules activos.

### 4. Ejecutar un Job (Manual)
```bash
# Ejecutar job tfm_current_load
databricks jobs run-now --job-name tfm_current_load

# Ejecutar job tfm_full_load
databricks jobs run-now --job-name tfm_full_load
```

### 5. Ver Estado de Jobs
```bash
# Listar todos los jobs del bundle
databricks bundle list

# Ver detalles de un job específico
databricks jobs get --job-name tfm_current_load

# Ver historial de ejecuciones
databricks jobs list-runs --job-name tfm_current_load
```

### 6. Destruir/Eliminar Recursos (Dev)
```bash
# Eliminar todos los recursos del bundle en dev
databricks bundle destroy --target dev

# Confirmación interactiva será solicitada
```

## Flujo de Materialización Recomendado

### Primera Vez (Setup Inicial)

1. **Preparar artefactos**:
   - Subir el wheel file: `processing_datalake-0.1-py3-none-any.whl`
   - Subir archivo de configuración: `conf-processing_datalake.tar.gz`
   - Configurar Databricks Secrets en workspace

2. **Validar configuración**:
   ```bash
   cd databricks_orchestation/databricks_jobs
   databricks bundle validate
   ```

3. **Ejecutar notebook de setup**:
   - Subir `Setup notebook.ipynb` a workspace
   - Ejecutarla en un cluster para crear esquemas y tablas iniciales

4. **Desplegar jobs en dev**:
   ```bash
   databricks bundle deploy --target dev
   ```

5. **Probar ejecución manual** en dev:
   ```bash
   databricks jobs run-now --job-name tfm_current_load
   ```

6. **Desplegar en prod**:
   ```bash
   databricks bundle deploy --target prod
   ```

### Despliegues Posteriores

```bash
# Simplemente redeploy
databricks bundle deploy --target prod
```

Los cambios en los archivos YAML se aplicarán en el próximo despliegue.

## Variables y Customización

Las variables definidas en `databricks.yml` pueden sobrescribirse:

```bash
# Desplegar con esquema diferente
databricks bundle deploy --target prod \
  -var="schema=custom_schema" \
  -var="catalog=other_catalog"
```

## Monitoreo y Logs

### Ver logs de una ejecución
```bash
# Obtener run_id de una ejecución
databricks jobs get-run <run_id>

# Ver logs de stdout
databricks runs get-output <run_id>
```

### Integración con Alertas
Los jobs están configurados para alertas de email. Configurar en Databricks workspace:
- Notificaciones en caso de fallo
- Alertas para runs saltadas

## Solución de Problemas

| Problema | Solución |
|----------|----------|
| `Authentication failed` | Verificar token en `~/.databrickscfg` o ejecutar `databricks configure --token` |
| `Invalid host` | Confirmar que el host en `databricks.yml` es correcto |
| `Wheel file not found` | Verificar que `processing_datalake-0.1-py3-none-any.whl` existe en workspace |
| `Secrets not found` | Crear secrets en Databricks: `databricks secrets put-secret scope key_name` |

## Referencias

- [Documentación Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Databricks CLI Documentation](https://docs.databricks.com/en/dev-tools/cli/)
- [Databricks Jobs API](https://docs.databricks.com/en/dev-tools/api/jobs/)

---

<div align="center">

### ⚡ Powered by Databricks

**Databricks Orchestration** - Automatiza tu pipeline de datos con Databricks Asset Bundles y la CLI oficial.

*Transforma datos en valor con la plataforma de datos más avanzada*

[Visita Databricks.com](https://databricks.com) | [Community Edition](https://www.databricks.com/try-databricks)

</div>

