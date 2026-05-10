# Fork/Join — ReciprocalArraySum

### Computación Paralela y Distribuida

---

## 📋 Tabla de contenidos

1. [El problema: ¿qué calcula el programa?](#1-el-problema-qué-calcula-el-programa)
2. [Versión secuencial vs paralela — vista rápida](#2-versión-secuencial-vs-paralela--vista-rápida)
3. [Teoría: ¿por qué paralelizar?](#3-teoría-por-qué-paralelizar)
4. [El patrón Fork–Join](#4-el-patrón-forkjoin)
5. [El Framework Fork/Join de Java](#5-el-framework-forkjoin-de-java)
6. [Cambio ADD #1 — Importar ForkJoinPool](#6-cambio-add-1--importar-forkjoinpool)
7. [Cambio ADD #2 — Completar `compute()` en `ReciprocalArraySumTask`](#7-cambio-add-2--completar-compute-en-reciprocalarraysumtask)
8. [Cambio ADD #3 — Implementar `parArraySum` (2 tareas)](#8-cambio-add-3--implementar-pararrays-2-tareas)
9. [Cambio ADD #4 — Implementar `parManyTaskArraySum` (N tareas)](#9-cambio-add-4--implementar-parmanytaskarrays-n-tareas)
10. [Cálculo de chunks — cómo se divide el arreglo](#10-cálculo-de-chunks--cómo-se-divide-el-arreglo)
11. [Work Stealing — el superpoder del ForkJoinPool](#11-work-stealing--el-superpoder-del-forkjoinpool)
12. [Conceptos clave para el examen](#12-conceptos-clave-para-el-examen)
13. [Comparación: código original vs modificado](#13-comparación-código-original-vs-modificado)
14. [Preguntas frecuentes del profesor](#14-preguntas-frecuentes-del-profesor)

---

## 1. El problema: ¿qué calcula el programa?

Dado un arreglo `input[]` de `double`, el programa calcula:

```
suma = 1/input[0] + 1/input[1] + 1/input[2] + ... + 1/input[n-1]
```

Es decir, la **suma de los recíprocos** (o inversos multiplicativos) de cada elemento.

### ¿Por qué este problema es ideal para paralelizar?

Porque cada `1/input[i]` es **independiente** de todos los demás. No hay dependencias de datos entre iteraciones. Esto lo convierte en un candidato perfecto para el patrón **Map → Reduce**:

```
MAP:    [1/a₀, 1/a₁, 1/a₂, ..., 1/aₙ₋₁]   ← cada uno independiente
REDUCE: suma_total = 1/a₀ + 1/a₁ + ... + 1/aₙ₋₁
```

---

## 2. Versión secuencial vs paralela — vista rápida

| Aspecto                         | `seqArraySum` | `parArraySum` (2 tareas) | `parManyTaskArraySum` (N tareas)      |
| ------------------------------- | ------------- | ------------------------ | ------------------------------------- |
| **Hilos usados**                | 1 (main)      | 2 (main + pool)          | N (main + N-1 del pool)               |
| **Estrategia**                  | Bucle simple  | fork/compute/join        | fork N-1 / compute 1 / join N-1       |
| **División del arreglo**        | No divide     | 2 mitades iguales        | N chunks con `ceil(n/N)`              |
| **Dónde corre la última tarea** | —             | En el hilo principal     | En el hilo principal                  |
| **Orden del join**              | —             | Solo 1 join              | LIFO (último forkeado, primero unido) |

---

## 3. Teoría: ¿por qué paralelizar?

### 3.1 El fin de la Ley de Moore (velocidad de reloj)

> _"La ley de Moore sobre la velocidad del reloj ha llegado a su fin."_  
> — Fox, Computer Architecture (2024)

Durante décadas, los procesadores se volvían más rápidos cada año porque aumentaban sus ciclos por segundo (GHz). Eso **ya no ocurre**. Hoy los chips tienen más transistores, pero cada núcleo individual no es más rápido que hace 15 años.

**Solución:** usar múltiples núcleos trabajando en paralelo (MIMD — Multiple Instruction, Multiple Data).

### 3.2 Analogía: la cocina del restaurante

Imagina que tienes que preparar 200 millones de platos:

```
Secuencial:  1 cocinero hace 200M platos → muy lento
Paralelo:    4 cocineros hacen 50M platos cada uno → ~4x más rápido
```

La clave es que preparar cada plato es **independiente** de los demás. Lo mismo pasa con calcular `1/input[i]`.

### 3.3 El pensamiento paralelo vs secuencial

```
SECUENCIAL (bucle):            PARALELO (map):
for i in datos:                pool.map(calcular, datos)
    calcular(datos[i])         ↑ cualquier orden, cualquier núcleo
    ← orden fijo, 1 a la vez
```

El bucle **impone un orden innecesario**. El map deja que la máquina decida el orden óptimo.

### 3.4 Speedup y Ley de Amdahl (concepto clave)

Si solo el `P%` del programa se puede paralelizar, el speedup máximo con `N` procesadores es:

```
Speedup = 1 / ((1 - P) + P/N)
```

**Ejemplo:** Si el 90% es paralelizable y tienes 4 núcleos:

```
Speedup = 1 / (0.10 + 0.90/4) = 1 / (0.10 + 0.225) = 1 / 0.325 ≈ 3.07x
```

Nunca obtienes 4x aunque tengas 4 núcleos, porque siempre hay alguna parte secuencial.

---

## 4. El patrón Fork–Join

### 4.1 Definición

> _"El patrón fork–join permite que el flujo de control se divida en múltiples flujos paralelos que posteriormente se vuelven a unir."_  
> — McCool et al., Structured Parallel Programming (2012)

### 4.2 Analogía: dividir y conquistar

```
Tarea grande
    │
    ├─── FORK ──────────────────────────────┐
    │                                       │
 Subtarea A                            Subtarea B
 (hilo del pool)                    (hilo principal)
    │                                       │
    └─────────── JOIN ──────────────────────┘
                    │
              Resultado combinado
```

Es como cuando el jefe de un proyecto divide el trabajo entre dos empleados, espera a que ambos terminen, y luego combina los resultados.

### 4.3 Flujo de ejecución — parArraySum (2 tareas)

```
Tiempo →

Hilo principal:  [crear LEFT] [crear RIGHT] [left.fork()──→] [right.compute()░░░░░░] [left.join()] [sum]
                                                                                                    ↑
Hilo del pool:                                               [──→ left.compute() ░░░░░░░░░░░] ────┘

                                              ↑ PARALELO ↑
                              Los dos compute() corren al mismo tiempo
```

**Clave:** `left.fork()` envía LEFT al pool de hilos (asíncrono) y el hilo principal **no espera** — inmediatamente ejecuta `right.compute()`. Así los dos trabajan **en paralelo**.

### 4.4 ¿Qué NO es fork–join?

| Concepto                    | ¿Es fork–join? | Diferencia                                                                    |
| --------------------------- | -------------- | ----------------------------------------------------------------------------- |
| **Barrera**                 | ❌ No          | Tras una barrera, TODOS los hilos continúan. Tras un join, solo UNO continúa. |
| **Thread.start() + join()** | Parcialmente   | No usa work-stealing; overhead mayor                                          |
| **ExecutorService**         | ❌ No          | No divide tareas recursivamente ni hace work-stealing                         |

### 4.5 Propiedad estructurada del grafo de tareas

El fork–join genera un grafo de tareas que es:

- **Limpiamente anidado** (nested): cada fork tiene exactamente un join correspondiente
- **Plano** (planar): el grafo puede dibujarse sin cruces

Esto permite razonar jerárquicamente sobre el programa.

---

## 5. El Framework Fork/Join de Java

### 5.1 Componentes principales

```
java.util.concurrent.ForkJoinPool
    └── gestiona hilos con work-stealing
    └── ejecuta objetos de tipo ForkJoinTask

ForkJoinTask (clase abstracta)
    ├── RecursiveTask<T>    ← devuelve resultado (usa getValue() o get())
    └── RecursiveAction     ← NO devuelve resultado (void)
                               ← ¡Esta es la que usamos aquí!
```

### 5.2 ¿Cuándo usar cuál?

| Necesito un resultado   | `RecursiveTask<T>`                |
| ----------------------- | --------------------------------- |
| Solo ejecuto una acción | `RecursiveAction`                 |
| Suma de recíprocos      | `RecursiveAction` + campo `value` |

`ReciprocalArraySumTask` extiende `RecursiveAction` y guarda el resultado en el campo `double value`, accesible mediante `getValue()`.

### 5.3 commonPool

Java tiene un pool de hilos compartido por toda la JVM llamado **commonPool**. Cuando llamas a `task.fork()`, la tarea se encola en este pool. El número de hilos del pool es aproximadamente igual al número de núcleos del procesador:

```java
ForkJoinPool.commonPool().getParallelism()
// típicamente = Runtime.getRuntime().availableProcessors() - 1
```

---

## 6. Cambio ADD #1 — Importar ForkJoinPool

### Código original

```java
import java.util.concurrent.RecursiveAction;
// Faltaba la importación de ForkJoinPool
```

### Código modificado (ADD #1)

```java
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ForkJoinPool;   // ← ADD #1
```

### ¿Por qué es necesario?

`ForkJoinPool` es la clase que gestiona el pool de hilos con work-stealing. Aunque en `parArraySum` no se crea explícitamente un `ForkJoinPool` (se usa el `commonPool` implícitamente a través de `fork()`), la importación es necesaria para que el compilador reconozca la clase en caso de uso explícito y para claridad del código.

---

## 7. Cambio ADD #2 — Completar `compute()` en `ReciprocalArraySumTask`

### Código original (incompleto)

```java
@Override
protected void compute() {
    // Para hacer  ← estaba vacío, no calculaba nada
}
```

### Código modificado (ADD #2)

```java
@Override
protected void compute() {
    double local = 0;
    for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
        local += 1.0 / input[i];   // Suma los recíprocos del chunk asignado
    }
    this.value = local;            // Guarda el resultado para getValue()
}
```

### Explicación detallada

Esta clase es la **unidad de trabajo** (la tarea). Cada instancia recibe:

- `startIndexInclusive` — índice de inicio del chunk (incluido)
- `endIndexExclusive` — índice de fin del chunk (excluido)
- `input[]` — el arreglo completo compartido

Y calcula la suma parcial de su chunk:

```
Chunk 0: input[0..99999]       → value = 1/input[0] + ... + 1/input[99999]
Chunk 1: input[100000..199999] → value = 1/input[100000] + ...
...
```

**¿Por qué `1.0 / input[i]` y no `1 / input[i]`?**  
Porque `1` es un entero en Java. `1 / input[i]` haría división entera si `input[i]` fuera entero. Con `1.0` se fuerza la división de punto flotante.

**¿Por qué guardar en `local` y luego en `this.value`?**  
Buena práctica: acumular en variable local es más eficiente que acceder al campo del objeto en cada iteración (evita indirección de memoria repetida).

---

## 8. Cambio ADD #3 — Implementar `parArraySum` (2 tareas)

### Código original (incompleto)

```java
protected static double parArraySum(final double[] input) {
    assert input.length % 2 == 0;
    double sum = 0;
    // Simplemente hacía el cálculo secuencial — ignoraba el paralelismo
    for (int i = 0; i < input.length; i++) {
        sum += 1 / input[i];
    }
    return sum;
}
```

### Código modificado (ADD #3)

```java
protected static double parArraySum(final double[] input) {
    assert input.length % 2 == 0;

    int mid = input.length / 2;

    // Crear dos tareas: LEFT (primera mitad) y RIGHT (segunda mitad)
    ReciprocalArraySumTask left  = new ReciprocalArraySumTask(0,   mid,          input);
    ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, input.length, input);

    left.fork();       // LEFT va al commonPool (asíncrono, no bloquea)
    right.compute();   // RIGHT corre en el hilo principal (síncrono)
    left.join();       // Espera a que LEFT termine

    return left.getValue() + right.getValue();
}
```

### Flujo de ejecución paso a paso

```
PASO 1: Crear LEFT  [0, mid)      y RIGHT [mid, n)
PASO 2: left.fork()  → LEFT sale volando al pool ══════════════════════════════╗
PASO 3: right.compute() ← hilo principal trabaja aquí mientras LEFT corre ►   ║
PASO 4: left.join()  ← esperamos a LEFT si aún no terminó                  ←══╝
PASO 5: LEFT.getValue() + RIGHT.getValue()
```

### ¿Por qué fork LEFT y compute RIGHT (y no al revés)?

Porque después de `fork()`, el hilo principal continúa. Si hiciéramos `right.fork()` y luego `left.compute()`, funcionaría igual. El patrón estándar es:

> **Forkea todo excepto la última tarea, y computa la última en el hilo actual.**

Esto garantiza que el hilo principal siempre esté haciendo trabajo útil en lugar de solo esperar.

### ¿Por qué el assert `input.length % 2 == 0`?

Para garantizar que la división en dos mitades iguales sea exacta. Si el arreglo tiene longitud impar, `mid` dejaría a un chunk con un elemento más que el otro, lo cual es válido en la práctica, pero el enunciado asume que es divisible por 2.

---

## 9. Cambio ADD #4 — Implementar `parManyTaskArraySum` (N tareas)

### Código original (incompleto)

```java
protected static double parManyTaskArraySum(final double[] input, final int numTasks) {
    double sum = 0;
    // Simplemente hacía el cálculo secuencial
    for (int i = 0; i < input.length; i++) {
        sum += 1 / input[i];
    }
    return sum;
}
```

### Código modificado (ADD #4)

```java
protected static double parManyTaskArraySum(final double[] input, final int numTasks) {

    // 1. Crear el arreglo de tareas
    ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
    for (int i = 0; i < numTasks; i++) {
        tasks[i] = new ReciprocalArraySumTask(
            getChunkStartInclusive(i, numTasks, input.length),
            getChunkEndExclusive(i, numTasks, input.length),
            input
        );
    }

    // 2. Fork TODAS menos la última
    for (int i = 0; i < numTasks - 1; i++) {
        tasks[i].fork();
    }

    // 3. Compute la última en el hilo principal
    tasks[numTasks - 1].compute();

    // 4. Acumular: empezar con el resultado de la última tarea
    double sum = tasks[numTasks - 1].getValue();

    // 5. Join en orden INVERSO (LIFO) y acumular
    for (int i = numTasks - 2; i >= 0; i--) {
        tasks[i].join();
        sum += tasks[i].getValue();
    }

    return sum;
}
```

### Flujo con N=4 tareas

```
TIEMPO →

Hilo principal:  [fork[0]] [fork[1]] [fork[2]] [compute[3]░░░░░░░░] [join[2]] [join[1]] [join[0]] [suma]
                                                                          ↑
Pool hilo A:     [░░░░░░░ task[0] ░░░░░░░░░░░░░░░░░░░░░░░░░░░░] ────────┤
Pool hilo B:         [░░░░ task[1] ░░░░░░░░░░░░░░░░░░░░░░░░░░░] ────────┤
Pool hilo C:             [░░░░ task[2] ░░░░░░░░░░░░░░░░░░░░░░░] ────────┘
                          ↑ PARALELO ↑
```

### ¿Por qué el join en orden LIFO (inverso)?

Este es un concepto **muy importante** del framework Fork/Join.

**Razón de eficiencia con work-stealing:**  
Las tareas forkeadas se agregan al frente de la deque (double-ended queue) del hilo que las creó. Cuando hacemos join en orden inverso, empezamos por la tarea más recientemente forkeada (la que tiene mayor probabilidad de haber sido ejecutada por el mismo hilo o de estar más cerca del tope de la deque). Esto minimiza la espera.

```
Fork en orden: [0] → [1] → [2]     ← se agregan al frente de la deque
Deque:         [2, 1, 0]
Join inverso:  join[2] → join[1] → join[0]  ← sacamos del frente primero
```

**Analogía:** Es como apilar platos. El último en apilarse es el primero en usarse (LIFO — Last In, First Out). Igual que la pila de llamadas (call stack) en recursión.

---

## 10. Cálculo de chunks — cómo se divide el arreglo

### Los tres métodos auxiliares

```java
// Tamaño de cada chunk (función techo)
private static int getChunkSize(int nChunks, int nElements) {
    return (nElements + nChunks - 1) / nChunks;
}

// Índice de inicio (inclusivo) del chunk k
private static int getChunkStartInclusive(int chunk, int nChunks, int nElements) {
    return chunk * getChunkSize(nChunks, nElements);
}

// Índice de fin (exclusivo) del chunk k
private static int getChunkEndExclusive(int chunk, int nChunks, int nElements) {
    int end = (chunk + 1) * getChunkSize(nChunks, nElements);
    return Math.min(end, nElements);  // evitar desbordamiento en el último chunk
}
```

### Ejemplo: N=10 elementos, K=3 tareas

```
chunkSize = ceil(10/3) = ceil(3.33) = 4

Chunk 0: start=0,  end=min(4,10)=4   → indices [0, 1, 2, 3]
Chunk 1: start=4,  end=min(8,10)=8   → indices [4, 5, 6, 7]
Chunk 2: start=8,  end=min(12,10)=10 → indices [8, 9]        ← último chunk puede ser más pequeño
```

```
Arreglo visual:
[0][1][2][3] | [4][5][6][7] | [8][9]
  Chunk 0         Chunk 1      Chunk 2
  (4 elem)        (4 elem)     (2 elem)
```

### ¿Por qué función techo (ceil) y no piso (floor)?

Si usáramos piso:

```
floor(10/3) = 3
Chunk 0: [0-2]   (3 elem)
Chunk 1: [3-5]   (3 elem)
Chunk 2: [6-8]   (3 elem)
¡Falta el elemento [9]! → resultado incorrecto
```

Con techo nunca perdemos elementos, aunque el último chunk sea más pequeño.

### Fórmula de techo entera sin `Math.ceil`

```java
ceil(a/b) = (a + b - 1) / b    // solo división entera
```

Esto es un truco clásico en programación de sistemas para evitar conversiones a `double`.

---

## 11. Work Stealing — el superpoder del ForkJoinPool

### ¿Qué es?

Cada hilo del pool tiene su propia **deque** (cola de doble extremo) de tareas pendientes. Cuando un hilo termina todas sus tareas, en lugar de quedarse ocioso, **roba** tareas de la deque de otro hilo ocupado.

```
Hilo A: [tarea1, tarea2, tarea3]  ← trabajando en tarea3
Hilo B: []                         ← ocioso, roba tarea1 de A

Resultado: A y B trabajan en paralelo en lugar de A hacer todo
```

### ¿Por qué roba del FINAL de la deque del otro hilo?

El hilo propietario trabaja en el **frente** (LIFO, las más recientes). El ladrón roba del **final** (FIFO, las más antiguas). Las tareas más antiguas tienden a ser más grandes (fueron forkeadas antes de subdividirse más), lo que hace más eficiente el robo.

```
Deque de Hilo A:
  FRENTE [tarea nueva] [tarea nueva] [TAREA GRANDE] FIN
  A trabaja → ←              B roba ←
```

### Diferencia con ExecutorService tradicional

| Característica       | `ForkJoinPool`                  | `ExecutorService`              |
| -------------------- | ------------------------------- | ------------------------------ |
| Work-stealing        | ✅ Sí                           | ❌ No                          |
| División recursiva   | ✅ Diseñado para ello           | ❌ Manual                      |
| Tareas sin resultado | `RecursiveAction`               | `Runnable`                     |
| Tareas con resultado | `RecursiveTask<T>`              | `Callable<T>`                  |
| Cola de tareas       | Deque por hilo                  | Cola global compartida         |
| Contención (lock)    | Baja (cada hilo tiene su deque) | Alta (todos comparten la cola) |

---

## 12. Conceptos clave

### 12.1 Recursividad y la Call Stack

> _"Para entender la recursividad, primero hay que entender las pilas."_  
> — Sweigart, The Recursive Book of Recursion (2022)

La **pila de llamadas** (call stack) es una estructura LIFO que:

- Agrega un **Frame** cuando se llama una función (push)
- Elimina el Frame cuando la función retorna (pop)
- Cada Frame guarda: dirección de retorno, argumentos, variables locales

```
Estado de la call stack al llamar a() → b() → c():

┌────────────────┐ ← tope
│ c()            │   frame de c
│ spam = 'Coyote'│
├────────────────┤
│ b()            │
│ spam = 'Lince' │
├────────────────┤
│ a()            │
│ spam = 'Hormiga'│
└────────────────┘
```

**Stack overflow:** ocurre cuando la recursión sin caso base llena completamente la memoria asignada a la call stack. En Python: límite de ~1000 llamadas. En JavaScript: ~10000.

### 12.2 Caso base vs Caso recursivo

Toda función recursiva tiene:

- **Caso base:** condición que detiene la recursión (retorna sin llamarse)
- **Caso recursivo:** llama a sí misma con un problema más pequeño

Sin caso base → stack overflow. Sin caso recursivo → función normal, no recursiva.

### 12.3 Estructuras LIFO y FIFO

| Estructura       | Orden | Analogía       | Uso en Fork/Join                  |
| ---------------- | ----- | -------------- | --------------------------------- |
| **Stack / Pila** | LIFO  | Pila de platos | Call stack, join en orden inverso |
| **Queue / Cola** | FIFO  | Fila del banco | Work-stealing roba del FIFO       |
| **Deque**        | Ambos | —              | Cola de tareas de cada hilo       |

### 12.4 Patrones de control paralelo

| Patrón         | ¿Qué hace?                                             | Ejemplo                   |
| -------------- | ------------------------------------------------------ | ------------------------- |
| **Fork–Join**  | Divide en subtareas paralelas que se reúnen            | `parArraySum`             |
| **Map**        | Aplica función elemental independiente a cada elemento | `1/input[i]` por cada i   |
| **Reduce**     | Combina colección en un valor con función asociativa   | Suma de todos los valores |
| **Map–Reduce** | Map + Reduce encadenados                               | El ejercicio completo     |
| **Scan**       | Reducciones parciales (prefijos)                       | Suma acumulativa          |
| **Stencil**    | Generalización de Map donde cada elem accede a vecinos | Filtros de imágenes       |

### 12.5 El patrón Map en este ejercicio

```
input:    [a₀,    a₁,    a₂,    ..., aₙ₋₁  ]
          ↓      ↓      ↓            ↓
map(1/x): [1/a₀, 1/a₁, 1/a₂, ..., 1/aₙ₋₁ ]
          ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
reduce(+): suma_total
```

La función elemental es `f(x) = 1/x` (pura, sin efectos secundarios). El operador de reducción es la suma (`+`), que es **asociativa** y **conmutativa**.

### 12.6 Asociatividad — ¿por qué importa?

La suma es asociativa: `(a + b) + c = a + (b + c)`. Esto significa que podemos reordenar las operaciones y dividirlas entre hilos sin cambiar el resultado.

⚠️ **Cuidado:** La suma en punto flotante **no es perfectamente asociativa** (hay errores de redondeo). Distintos ordenamientos pueden dar resultados ligeramente diferentes. Esto es una limitación conocida de la paralelización con flotantes.

### 12.7 SIMD vs MIMD — clasificación de paralelismo

| Tipo     | Significado                         | Ejemplo                         |
| -------- | ----------------------------------- | ------------------------------- |
| **SIMD** | Single Instruction, Multiple Data   | GPU procesando píxeles          |
| **MIMD** | Multiple Instruction, Multiple Data | Múltiples núcleos con ForkJoin  |
| **SPMD** | Single Program, Multiple Data       | Mismo programa, datos distintos |

Este ejercicio es **MIMD** (cada hilo puede estar en una parte diferente del programa) pero como todos ejecutan el mismo `compute()`, también podría llamarse **SPMD**.

### 12.8 RecursiveAction — por qué no retorna valor directamente

`RecursiveAction.compute()` es `void`. Para obtener resultados, la tarea guarda el resultado en un campo y el llamador lo recupera después del `join()`:

```java
// En la tarea:
private double value;
protected void compute() { this.value = ...; }
public double getValue() { return value; }

// En el llamador:
task.fork();
// ... hacer otro trabajo ...
task.join();          // esperar
task.getValue();      // ahora es seguro leer el resultado
```

Esto es un patrón similar a `Future<T>` pero más ligero.

---

## 13. Comparación: código original vs modificado

### Resumen de los 4 cambios

```
CAMBIO    DÓNDE                    QUÉ SE AGREGÓ               IMPACTO
────────────────────────────────────────────────────────────────────────────
ADD #1    Importaciones            import ForkJoinPool           Habilita pool
ADD #2    ReciprocalArraySumTask   Implementación de compute()   Calcula parcial
ADD #3    parArraySum              fork/compute/join (2 tareas)  Paralelismo 2x
ADD #4    parManyTaskArraySum      fork N-1 / compute / join N   Paralelismo Nx
```

### Línea a línea — parArraySum

```java
// ANTES (secuencial disfrazado de paralelo):
double sum = 0;
for (int i = 0; i < input.length; i++) { sum += 1 / input[i]; }
return sum;

// DESPUÉS (paralelo real):
int mid = input.length / 2;                                    // ← divide en 2
ReciprocalArraySumTask left  = new ReciprocalArraySumTask(...);// ← crea tarea izq
ReciprocalArraySumTask right = new ReciprocalArraySumTask(...);// ← crea tarea der
left.fork();        // ← izquierda va al pool (no bloquea)
right.compute();    // ← derecha corre AHORA en hilo principal (PARALELO con left)
left.join();        // ← espera que izquierda termine
return left.getValue() + right.getValue(); // ← combina resultados
```

### Línea a línea — parManyTaskArraySum

```java
// ANTES (mismo bucle secuencial de siempre):
double sum = 0;
for (int i = 0; i < input.length; i++) { sum += 1 / input[i]; }
return sum;

// DESPUÉS (N tareas en paralelo):
ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks]; // arreglo de tareas

for (int i = 0; i < numTasks; i++) {                          // crear todas las tareas
    tasks[i] = new ReciprocalArraySumTask(
        getChunkStartInclusive(i, numTasks, input.length),    // inicio del chunk i
        getChunkEndExclusive(i, numTasks, input.length),      // fin del chunk i
        input
    );
}

for (int i = 0; i < numTasks - 1; i++) { tasks[i].fork(); }  // forkear N-1 tareas
tasks[numTasks - 1].compute();                                // compute la última acá

double sum = tasks[numTasks - 1].getValue();                  // resultado de la última
for (int i = numTasks - 2; i >= 0; i--) {                    // join en orden LIFO
    tasks[i].join();
    sum += tasks[i].getValue();
}
return sum;
```

---

## 14. Preguntas frecuentes

**P: ¿Por qué se usa `RecursiveAction` si el ejercicio no tiene recursión?**  
R: `RecursiveAction` no exige que la tarea se llame recursivamente. Es simplemente la clase base del framework para tareas sin valor de retorno. Su nombre viene del caso de uso más común (divide y vencerás recursivo), pero aquí la usamos de forma "plana" (sin subdivisión recursiva).

---

**P: ¿Qué pasa si llamas a `join()` antes de que la tarea haya terminado?**  
R: El hilo que llama a `join()` se **bloquea** (se suspende) hasta que la tarea complete. Esto es un mecanismo de sincronización: garantiza que `getValue()` solo se lea cuando el valor ya fue calculado.

---

**P: ¿Qué diferencia hay entre `fork()` y `compute()`?**

| Método      | Dónde corre la tarea            | ¿Bloquea el llamador?    |
| ----------- | ------------------------------- | ------------------------ |
| `fork()`    | En un hilo del pool (asíncrono) | ❌ No                    |
| `compute()` | En el hilo actual (síncrono)    | ✅ Sí, hasta que termina |
| `invoke()`  | En el hilo actual o pool        | ✅ Sí                    |

---

**P: ¿Por qué el join va de `numTasks-2` hasta `0` y no de `0` a `numTasks-2`?**  
R: Por eficiencia con el mecanismo de work-stealing. Las tareas más recientemente forkeadas están al tope de la deque. Hacer join en orden inverso (empezar por la más reciente) reduce la espera porque esas tareas son las más probables de ya haber terminado o de estar ejecutándose.

---

**P: ¿Es la suma de recíprocos en paralelo exactamente igual a la secuencial?**  
R: No necesariamente. La suma de punto flotante no es perfectamente asociativa. El resultado paralelo puede diferir en los últimos bits por errores de redondeo acumulados en diferente orden. Para el propósito del ejercicio (comparar resultados con tolerancia), se acepta esta diferencia.

---

**P: ¿Qué es el commonPool y cuántos hilos tiene?**  
R: El `commonPool` es el pool compartido por toda la JVM. Su número de hilos es `Runtime.getRuntime().availableProcessors() - 1`. En un procesador de 4 núcleos, tendrá 3 hilos en el pool (más el hilo principal, son 4 en total).

---

**P: ¿Cuál es la diferencia entre una barrera y un join?**  
R: Tras una **barrera**, TODOS los hilos participantes continúan. Tras un **join**, solo el hilo que llamó a `join()` continúa; el hilo de la subtarea finaliza. Fork–join es jerárquico; las barreras son de sincronización horizontal entre peers.

---

**P: ¿Por qué `getChunkSize` usa `(nElements + nChunks - 1) / nChunks` en lugar de `Math.ceil((double)nElements/nChunks)`?**  
R: Para evitar conversión a `double` y posibles pérdidas de precisión. La fórmula `(a + b - 1) / b` calcula el techo de la división entera `a/b` usando solo aritmética entera, que es exacta y más rápida.

---

**P: ¿El framework Fork/Join usa SIMD o MIMD?**  
R: **MIMD** (Multiple Instruction, Multiple Data). Cada hilo tiene su propio contador de programa y puede estar ejecutando instrucciones distintas al mismo tiempo. El `ForkJoinPool` ejecuta tareas Java en hilos del sistema operativo, que a su vez corren en núcleos físicos distintos.

---

**P: ¿Qué es work-stealing y por qué es importante?**  
R: Es el mecanismo por el que hilos ociosos "roban" tareas de la deque de hilos ocupados. Es importante porque garantiza alta utilización de CPU: ningún núcleo queda ocioso mientras hay trabajo disponible. Los ladrones roban del final de la deque (tareas más antiguas y grandes) para maximizar la cantidad de trabajo robado por acción de robo.

---

## Referencias bibliográficas

- Sweigart, A. _The Recursive Book of Recursion_. No Starch Press. 2022.
- McCool, M.; Robison, A.D.; Reinders, J. _Structured Parallel Programming_. Elsevier. 2012.
- Fox, C. _Computer Architecture. From the Stone Age to the Quantum Age_. No Starch Press. 2024.
- Oracle. _Java SE Documentation: ForkJoinPool, RecursiveAction, RecursiveTask_. docs.oracle.com
- Bytecoders. _Java Fork/Join Framework Explained_. Medium. Diciembre 2025.
