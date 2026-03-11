# Proyecto T2: Sneaker Point Retail Analytics

## 1. Contexto del proyecto

Sneaker Point es una cadena de tiendas físicas de zapatillas en España. La compañía dispone de una base de datos PostgreSQL operativa donde se registran productos, variantes, clientes, ventas, devoluciones, descuentos, stock, reposiciones y movimientos de inventario.

La dirección quiere construir una base analítica seria y reutilizable para explotar el negocio con criterios reales de BI. Vuestro equipo ha sido contratado como **consultoría de analítica e ingeniería de datos**.

Vuestra misión será construir un proyecto completo de datos con enfoque moderno, desde la **ingesta** hasta la **capa Gold**, siguiendo una arquitectura por capas y buenas prácticas de calidad, documentación y mantenimiento. Igual que en la PT4, no basta con transformar datos sin criterio: debéis trabajar con fuentes reales, modelado por capas, histórico, incrementales, tests y documentación. 

---

## 2. Objetivo general

Debéis desarrollar un proyecto analítico completo que:

* extraiga datos desde PostgreSQL usando **dlt**,
* los deje disponibles en una capa base o landing,
* construya una capa **Silver** limpia, consistente y estandarizada,
* construya una capa **Gold** orientada a negocio,
* implemente **tests** de calidad,
* incluya **documentación** de modelos y columnas,
* incluya al menos **1 snapshot**,
* incluya al menos **1 modelo incremental**,
* y deje el proyecto preparado para evolucionar como si fuera un entorno real.

---

## 3. Tecnología y restricción importante

### Ingesta obligatoria

La extracción desde PostgreSQL debe realizarse usando **dlt**.

### Importante

No se os dará una guía paso a paso de dlt. Debéis:

* investigar qué es,
* entender cómo se usa para extraer desde PostgreSQL,
* justificar brevemente vuestras decisiones,
* y conseguir que la ingesta funcione correctamente.

La parte de investigación forma parte del proyecto.

---

## 4. Fuente de datos

Trabajaréis contra una base de datos PostgreSQL accesible en remoto.

### Datos de conexión

* **Host:** `vps-3a29d7f8.vps.ovh.net`
* **Puerto:** `5432`
* **Base de datos:** `proyecto_dbt`
* **Usuario:** `alumno`
* **Password:** `1234`

### Ejemplo de conexión

Podéis usar esos datos para configurar vuestra conexión desde dlt, desde un cliente SQL o desde el perfil que necesitéis en vuestro entorno.

---

## 5. Ayuda funcional: GPT cliente

Para este proyecto tenéis disponible un GPT que simula ser el cliente técnico de Sneaker Point:

**GPT cliente:**
`https://chatgpt.com/g/g-69aef43c55a48191ab93c6f05c551410-cliente-tecnico-sneaker-point`

### Para qué debéis usarlo

Debéis usar ese GPT como si fuera vuestro cliente real para:

* preguntar qué espera en la capa Gold,
* aclarar el significado funcional de tablas y columnas,
* resolver dudas de negocio,
* validar definiciones de métricas,
* contrastar granularidades,
* y entender mejor qué necesita dirección, operaciones o retail.

### Importante

Ese GPT **no sustituye vuestro criterio técnico**. Debéis usarlo como apoyo funcional, no como excusa para dejar de pensar. Si el GPT os sugiere algo, debéis validar si tiene sentido con los datos reales y con vuestro modelo.

---

## 6. Esquema del negocio

El dominio funcional incluye, entre otros, elementos como:

* tiendas,
* horarios de apertura,
* marcas,
* categorías,
* proveedores,
* clientes,
* empleados,
* productos,
* variantes de producto,
* stock por tienda y variante,
* descuentos,
* ventas,
* líneas de venta,
* pagos,
* devoluciones,
* reembolsos,
* pedidos a proveedor,
* movimientos de stock.

La tienda vende de **lunes a sábado de 09:00 a 21:00**. Los domingos no se consideran operativos.

---

## 7. Qué debéis construir

## 7.1 Ingesta

Debéis construir una ingesta desde PostgreSQL usando **dlt**.

### Requisitos mínimos

* Debéis extraer las tablas de origen necesarias desde PostgreSQL.
* Debéis dejar clara vuestra capa de entrada o landing.
* Debéis documentar qué tablas habéis ingerido y por qué.
* Debéis explicar brevemente vuestra estrategia de carga.

No se evalúa solo “que funcione”, sino también que esté bien planteado.

---

## 7.2 Capa Silver

Debéis construir una capa **Silver** con modelos intermedios, normalmente con prefijo `stg_` o el que decidáis, siempre que la convención sea clara y coherente.

### Objetivo de Silver

Dejar los datos listos para análisis y consumo posterior.

### En Silver debéis resolver, cuando aplique

* tipos incorrectos,
* nulos problemáticos,
* duplicados,
* incoherencias de formato,
* relaciones rotas,
* códigos inconsistentes,
* limpieza de texto,
* normalización de campos,
* columnas derivadas útiles,
* y cualquier otro problema de calidad que detectéis.

### Importante

No se trata de copiar tablas. Se trata de **limpiar, estandarizar y preparar**.

---

## 7.3 Snapshot obligatorio

Debéis implementar al menos **1 snapshot** sobre una entidad que cambie con el tiempo.

### Ejemplos razonables

* clientes,
* productos,
* variantes,
* precios,
* descuentos,
* o cualquier otra entidad cuyo histórico tenga valor analítico.

### Qué se espera

* que el snapshot tenga sentido funcional,
* que capture cambios reales,
* y que podáis explicar por qué merece conservar histórico.

---

## 7.4 Incremental obligatorio

Debéis implementar al menos **1 modelo incremental**.

### Qué se espera

* que el modelo sea coherente con el comportamiento del dato,
* que evite reprocesar todo innecesariamente,
* que tenga una lógica clara de actualización,
* y que podáis justificar por qué ese modelo debe ser incremental.

### Ejemplos razonables

* ventas,
* líneas de venta,
* movimientos de stock,
* pagos,
* devoluciones,
* o agregados crecientes en el tiempo.

---

## 7.5 Capa Gold

Debéis construir una capa **Gold** orientada a negocio, no solo a técnica.

La capa Gold debe responder preguntas reales del negocio de Sneaker Point, como por ejemplo:

* qué vende más,
* qué tiendas rinden mejor,
* cuánto impactan los descuentos,
* qué volumen de devoluciones existe,
* qué clientes repiten,
* cómo se comporta el stock,
* dónde hay roturas de stock,
* qué marcas o categorías funcionan mejor,
* o qué KPIs ejecutivos necesita dirección.

### Requisito general

Debéis diseñar una Gold que tenga sentido funcional y que esté claramente separada de Silver.

### Mínimos obligatorios

Debéis entregar, como mínimo:

* **al menos 3 dimensiones**
* **al menos 2 hechos**
* **al menos 1 agregado de negocio**

### Sugerencias posibles

No son obligatorias, pero pueden serviros como referencia:

**Dimensiones**

* `dim_date`
* `dim_store`
* `dim_customer`
* `dim_product`
* `dim_product_variant`
* `dim_discount`

**Hechos**

* `fct_sales`
* `fct_sales_line`
* `fct_returns`
* `fct_stock_movements`

**Agregados**

* ventas diarias por tienda
* KPIs de clientes
* resumen de devoluciones
* cobertura o rotura de stock
* rendimiento por marca y categoría

### Importante

No se evaluará solo el número de modelos, sino si el diseño **sirve para negocio**.

---

## 8. Uso obligatorio del GPT cliente en la definición de Gold

Antes de cerrar vuestra propuesta de Gold, debéis consultar el GPT cliente.

### Debéis usarlo para:

* preguntar qué métricas espera,
* validar la granularidad de vuestras facts,
* entender diferencias entre producto y variante,
* aclarar cómo tratar ventas netas, descuentos y devoluciones,
* y contrastar qué entregables mínimos esperaría un cliente real.

### Evidencia mínima

En la memoria o README debéis incluir una breve sección indicando:

* qué preguntas le hicisteis,
* qué respuestas os ayudaron,
* y qué decisiones de modelado tomasteis a partir de eso.

---

## 9. Tests obligatorios

Debéis incluir **tests de calidad** en dbt.

### Como mínimo

* `not_null`
* `unique`
* `relationships`

### Además

Debéis añadir tests adicionales cuando tenga sentido, por ejemplo:

* `accepted_values`
* unicidad compuesta,
* reglas de negocio simples,
* validaciones de importes positivos,
* validaciones de cantidades no negativas,
* o cualquier comprobación útil.

### Dónde deben aparecer

Los tests deben existir, cuando tenga sentido, en:

* sources,
* modelos Silver,
* snapshots,
* dimensiones,
* hechos,
* agregados Gold.

No se trata de llenar el proyecto de tests vacíos, sino de aplicarlos con criterio.

---

## 10. Documentación obligatoria

Debéis documentar el proyecto de forma seria.

### Como mínimo debéis documentar

* sources,
* modelos Silver principales,
* snapshot(s),
* incremental(es),
* dimensiones Gold,
* hechos Gold,
* agregados finales,
* columnas clave,
* y el grano de cada fact.

### Requisito

La documentación debe servir para que otra persona entienda:

* qué hace cada modelo,
* qué representa cada columna importante,
* de dónde viene el dato,
* y cómo debe usarse.

El proyecto debe quedar preparado para ejecutar `dbt docs`.

---

## 11. Qué se espera de vuestra propuesta funcional

No recibiréis una lista cerrada de modelos Gold obligatorios. Igual que en un proyecto real, debéis decidir.

### Debéis justificar:

* qué preguntas de negocio queréis responder,
* qué métricas vais a exponer,
* qué dimensiones usará el análisis,
* cuál es el grano de cada fact,
* qué histórico necesitáis,
* y por qué vuestra Gold tiene valor real.

---

## 12. Entregables obligatorios

Debéis entregar un repositorio completo que incluya como mínimo:

* proyecto dbt funcional,
* ingesta con dlt desde PostgreSQL,
* sources declaradas,
* modelos Silver,
* al menos 1 snapshot,
* al menos 1 incremental,
* capa Gold,
* tests,
* documentación,
* README,
* y una breve explicación funcional del proyecto.

### El README debe incluir al menos

* objetivo del proyecto,
* arquitectura por capas,
* cómo ejecutar la ingesta,
* cómo ejecutar dbt,
* qué modelos principales habéis creado,
* qué snapshot habéis elegido,
* qué incremental habéis elegido,
* qué decisiones habéis tomado en Gold,
* y cómo habéis usado el GPT cliente.