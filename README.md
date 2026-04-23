# TP Coordinación - Aizen Sanchez 110944

## Informe de Coordinación Implementada

La coordinación del sistema está centrada en las instancias de Sum. Cada Sum mantiene estado por cliente (acumulación por fruta y conteo de mensajes) y, cuando recibe EOF, no libera datos inmediatamente hacia Aggregation: primero inicia una fase de control para verificar que la ingesta global del cliente esté completa.

Para eso, Sum usa una capa de control separada (SumControlLayer) que coordina a todas las instancias de Sum mediante mensajes de control. El flujo es:

1. Se solicita el conteo local de cada Sum para un cliente.
2. Cada instancia responde su cantidad local procesada.
3. Se consolida un conteo global y se valida contra el total esperado informado por Gateway.
4. Solo cuando la validación coincide, se habilita la publicación de datos hacia Aggregation.

Además, el envío de datos desde Sum a Aggregation está particionado por hash de (cliente, fruta), evitando broadcast completo y reduciendo procesamiento redundante. Esto permite escalar en cantidad de instancias sin duplicar trabajo innecesario.

En Aggregation, cada instancia consolida los datos que le corresponden y mantiene un top parcial por cliente. La sincronización se da esperando EOF de todas las instancias de Sum para ese cliente; recién en ese punto calcula y publica su top parcial hacia Join.

En Join, la coordinación final consiste en esperar los tops parciales de todas las instancias de Aggregation para cada cliente. A medida que llegan, los fusiona incrementalmente y, cuando recibe todos los esperados, genera el top final y lo envía al Gateway.

En Gateway, el archivo message_handler cumple un rol clave de correlación: asigna un identificador único por cliente, serializa mensajes internos incluyendo ese identificador y un contador total de registros enviados, y al recibir resultados valida que pertenezcan al cliente correcto. Esto garantiza aislamiento entre consultas concurrentes y permite que las fases de coordinación en Sum, Aggregation y Join operen correctamente por cliente.
