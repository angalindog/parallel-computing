package co.edu.unal.paralela;
import static edu.rice.pcdp.PCDP.forseq2d;
// ---------------------------------------------------------------------------
import static edu.rice.pcdp.PCDP.forallChunked;
// --------------------------------------------------------------------------- ADD #1

/**
 * Clase envolvente pata implementar de forma eficiente la multiplicación dde matrices en paralelo.
 */
public final class MatrixMultiply {
    /**
     * Constructor por omisión.
     */
    private MatrixMultiply() {
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma secuencial.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N Tamaño de las matrices de entrada
     */
    public static void seqMatrixMultiply(final double[][] A, final double[][] B,
                                         final double[][] C, final int N) {
        forseq2d(0, N - 1, 0, N - 1, (i, j) -> {
            C[i][j] = 0.0;
            for (int k = 0; k < N; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        });
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma paralela.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N amaño de las matrices de entrada
     */
    public static void parMatrixMultiply(final double[][] A, final double[][] B,
                                         final double[][] C, final int N) {
/*
         * PARA HACER: paralelizar el ciclo externo para mejorar el desempeño.


        forseq2d(0, N - 1, 0, N - 1, (i, j) -> {
            C[i][j] = 0.0;
            for (int k = 0; k < N; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        });
*/

        // ---------------------------------------------------------------------------
    // Paralelizamos únicamente el ciclo externo de las filas (i)
            forallChunked(0, N - 1, i -> {

                // 1. Inicializamos la fila entera en cero de forma secuencial
                for (int j = 0; j < N; j++) { // Se recorren las columnas j de la fila i mediante un ciclo for
                    C[i][j] = 0.0;
                }

                // 2. Multiplicación optimizada para caché (Orden: i -> k -> j)
                for (int k = 0; k < N; k++) {
                    // Almacenamos en un registro local para evitar lecturas redundantes de A
                    double aVal = A[i][k];

                    // El ciclo interno 'j' recorre la memoria de forma contigua (stride-1)
                    for (int j = 0; j < N; j++) {
                        C[i][j] += aVal * B[k][j];
                    }
                }
            });
        // --------------------------------------------------------------------------- ADD #2

    }
}
