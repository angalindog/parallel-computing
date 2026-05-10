package co.edu.unal.paralela;
import java.util.concurrent.RecursiveAction;
// ----------------------------------------
import java.util.concurrent.ForkJoinPool;
// ---------------------------------------- ADD #1
/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de un arreglo usando paralelismo.
 */
public final class ReciprocalArraySum {
    /**
     * Constructor.
     */
    private ReciprocalArraySum() {
    }
    /**
     * Calcula secuencialmente la suma de valores recíprocos para un arreglo.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;
// Calcula la suma de los recíprocos de los elementos del arreglo
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }
        return sum;
    }
    /**
     * calcula el tamaño de cada trozo o sección, de acuerdo con el número de secciones para crear
     * a través de un número dado de elementos.
     *
     * @param nChunks El número de secciones (chunks) para crear
     * @param nElements El número de elementos para dividir
     * @return El tamaño por defecto de la sección (chunk)
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
// Función techo entera
        return (nElements + nChunks - 1) / nChunks;
    }
    /**
     * Calcula el índice del elemento inclusivo donde la sección/trozo (chunk) inicia,
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk la sección/trozo (chunk) para calcular la posición de inicio
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice inclusivo donde esta sección/trozo (chunk) inicia en el conjunto de
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }
    /**
     * Calcula el índice del elemento exclusivo que es proporcionado al final de la sección/trozo (chunk),
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk La sección para calcular donde termina
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice de terminación exclusivo para esta sección/trozo (chunk)
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }
    /**
     * Este pedazo de clase puede ser completada para para implementar el cuerpo de cada tarea creada
     * para realizar la suma de los recíprocos del arreglo en paralelo.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Iniciar el índice para el recorrido transversal hecho por esta tarea.
         */
        private final int startIndexInclusive;
        /**
         * Concluir el índice para el recorrido transversal hecho por esta tarea.
         */
        private final int endIndexExclusive;
        /**
         * Arreglo de entrada para la suma de recíprocos.
         */
        private final double[] input;
        /**
         * Valor intermedio producido por esta tarea.
         */
        private double value;
        /**
         * Constructor.
         * @param setStartIndexInclusive establece el índice inicial para comenzar
         * el recorrido transversal.
         * @param setEndIndexExclusive establece el índice final para el recorrido transversal.
         * @param setInput Valores de entrada
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }
        /**
         * Adquiere el valor calculado por esta tarea.
         * @return El valor calculado por esta tarea
         */
        public double getValue() {
            return value;
        }
        @Override
        protected void compute() {
// Para hacer
// ---------------------------------------------------
            double local = 0; // local es una variable local que se guarda en el stack -- Fácil acceso
            for (int i = startIndexInclusive; i < endIndexExclusive; i++){ // Se especifican los índices de inicio y fin para cada tarea
                local += 1.0 / input[i]; // Se calcula la suma de los recíprocos para el rango específico de esta tarea
            }
            this.value = local; // Se asigna el resultado a la variable de instancia value, que está en el heap que es más lento de acceder, pero es compartida entre tareas, lo que permite que el resultado de cada tarea se pueda combinar posteriormente, sería más lento si value se usara para guardar el resultado de cada tarea, ya que se tendría que acceder al heap cada vez, lo que es más lento que acceder a una variable local en el stack.
// ---------------------------------------------------- ADD #2
        }
    }
    /**
     * Para hacer: Modificar este método para calcular la misma suma de recíprocos como le realizada en
     * seqArraySum, pero utilizando dos tareas ejecutándose en paralelo dentro del framework ForkJoin de Java
     * Se puede asumir que el largo del arreglo de entrada
     * es igualmente divisible por 2.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;
// ---------------------------------------------------------------------
        int mid = input.length /2; // Se calcula el punto medio del arreglo para dividirlo en dos partes iguales
        ReciprocalArraySumTask left = new ReciprocalArraySumTask(0, mid, input); // Se crea una tarea para la primera mitad del arreglo
        ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, input.length, input); // Se crea una tarea para la segunda mitad del arreglo
        left.fork(); // Se ejecuta la primera tarea en paralelo
        right.compute(); // Se ejecuta la segunda tarea en el hilo actual, lo que permite que ambas tareas se ejecuten en paralelo
        left.join(); // Se espera a que la primera tarea termine para poder obtener su resultado
        return left.getValue() + right.getValue(); // Se suma el resultado de ambas tareas para obtener la suma total de los recíprocos del arreglo de entrada
// --------------------------------------------------------------------- ADD #3
    }
    /**
     * Para hacer: extender el trabajo hecho para implementar parArraySum que permita utilizar un número establecido
     * de tareas para calcular la suma del arreglo recíproco.
     * getChunkStartInclusive y getChunkEndExclusive pueden ser útiles para calcular
     * el rango de elementos índice que pertenecen a cada sección/trozo (chunk).
     *
     * @param input Arreglo de entrada
     * @param numTasks El número de tareas para crear
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {
// --------------------------------------------------------------------------
        ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks]; // Se crea un arreglo de tareas para almacenar las tareas que se van a crear
        for (int i = 0; i < numTasks; i++){
            tasks[i] = new ReciprocalArraySumTask( // Se crean las tareas para cada sección del arreglo utilizando los métodos getChunkStartInclusive y getChunkEndExclusive para determinar los índices de inicio y fin de cada sección
                    getChunkStartInclusive(i, numTasks, input.length),// Se calcula el índice de inicio para la sección i utilizando el método getChunkStartInclusive, que toma en cuenta el número total de tareas y el tamaño del arreglo de entrada para determinar el punto de inicio de cada sección
                    getChunkEndExclusive(i, numTasks, input.length),
                    input // Se calcula el índice de fin para la sección i utilizando el método getChunkEndExclusive, que también toma en cuenta el número total de tareas y el tamaño del arreglo de entrada para determinar el punto de fin de cada sección
            );
        }
        for (int i = 0; i < numTasks -1 ; i++){ // Se ejecuta cada tarea en paralelo excepto la última, que se ejecuta en el hilo actual para permitir que todas las tareas se ejecuten en paralelo
            tasks[i].fork(); // Se ejecuta la tarea i en paralelo utilizando el método fork, lo que permite que cada tarea se ejecute en un hilo separado y se aproveche el paralelismo para calcular la suma de los recíprocos de cada sección del arreglo de entrada
        }
        tasks[numTasks -1].compute(); // Se ejecuta la última tarea en el hilo actual

        double sum = tasks[numTasks -1].getValue(); // Se obtiene el resultado de la última tarea, que se ejecutó en el hilo actual, para iniciar la suma total de los recíprocos del arreglo de entrada
        for (int i = numTasks -2; i>=0; i--) { // Se espera a que cada tarea termine utilizando el método join para poder obtener su resultado y sumarlo al resultado total, comenzando desde la penúltima tarea hasta la primera, ya que la última tarea se ejecutó en el hilo actual y su resultado ya se obtuvo
            tasks[i].join();
            sum += tasks[i].getValue(); // Se suma el resultado de la tarea i al resultado total
        }
        return sum; // Se devuelve el resultado total de la suma de los recíprocos del arreglo de entrada
// -------------------------------------------------------------------------- ADD #4
    }
}


