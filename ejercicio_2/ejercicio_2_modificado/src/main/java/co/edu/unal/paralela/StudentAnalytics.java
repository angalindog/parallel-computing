package co.edu.unal.paralela;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;

//----------------------------------------------------------------
import java.util.stream.Collectors; // para Collectors.groupingBy()
//---------------------------------------------------------------- ADD #1


/**
 * Una clase 'envoltorio' (wrapper) para varios métodos analíticos.
 */
public final class StudentAnalytics {
    /**
     * Calcula secuencialmente la edad promedio de todos los estudiantes registrados y activos
     * utilizando ciclos.
     *
     * @param studentArray Datos del estudiante para la clase.
     * @return Edad promedio de los estudiantes registrados
     */
    public double averageAgeOfEnrolledStudentsImperative(
            final Student[] studentArray) {
        List<Student> activeStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (s.checkIsCurrent()) {
                activeStudents.add(s);
            }
        }

        double ageSum = 0.0;
        for (Student s : activeStudents) {
            ageSum += s.getAge();
        }

        return ageSum / (double) activeStudents.size();
    }

    /**
     * PARA HACER calcular la edad promedio de todos los estudiantes registrados y activos usando
     * streams paralelos. Debe reflejar la funcionalidad de 
     * averageAgeOfEnrolledStudentsImperative. Este método NO debe utilizar ciclos.
     *
     * @param studentArray Datos del estudiante para esta clase.
     * @return Edad promedio de los estudiantes registrados
     */
    public double averageAgeOfEnrolledStudentsParallelStream(
            final Student[] studentArray) {

        //-------------------------------------------------------------------------------------
        return Stream.of(studentArray) // Convierte el array en un stream secuencial
                .parallel()  // Le indica al framework fork/join de java a dividir el stream en subtareas y procesarlas en varios hilos según los núcleos paralelos. Internamente, usa el ForkJoinPool común.
                .filter(Student::checkIsCurrent) // Filtra los estudiantes activos, es una referencia de método equivalente a lambda s -> s.checkIsCurrent(), y descarta los estudiantes inactivos. Aquí no se requiere de ciclo for, y se aplica concurrentemente a cada segmento del arreglo.
                .mapToDouble(Student::getAge)  // transforma el stream<student> en doubleStream extrayendo la edad de cada estudiante, es una referencia de método equivalente a lambda s -> s.getAge(), que luego se empleara en el calculo del promedio con Average.
                .average()  // Es la operación final que suma todas las edades y las divide por la cantidad de estudiantes activos de forma paralela, retorna un optional Double
                .orElse(0.0); // Se encarga de desenvolver el optional double; en caso de que el stream estuviera vacío devuelve 0.0 en lugar de arrojar una excepción.
        //throw new UnsupportedOperationException();
        //------------------------------------------------------------------------------------- ADD #2

    }

    /**
     * Calcula secuencialmente -usando ciclos- el nombre más común de todos los estudiantes 
     * que no están activos en la clase.
     *
     * @param studentArray Datos del estudiante para esta clase.
     * @return Nombre más común de los estudiantes inactivos.
     */
    public String mostCommonFirstNameOfInactiveStudentsImperative(
            final Student[] studentArray) {
        List<Student> inactiveStudents = new ArrayList<Student>();

        for (Student s : studentArray) {
            if (!s.checkIsCurrent()) {
                inactiveStudents.add(s);
            }
        }

        Map<String, Integer> nameCounts = new HashMap<String, Integer>();

        for (Student s : inactiveStudents) {
            if (nameCounts.containsKey(s.getFirstName())) {
                nameCounts.put(s.getFirstName(),
                        new Integer(nameCounts.get(s.getFirstName()) + 1));
            } else {
                nameCounts.put(s.getFirstName(), 1);
            }
        }

        String mostCommon = null;
        int mostCommonCount = -1;
        for (Map.Entry<String, Integer> entry : nameCounts.entrySet()) {
            if (mostCommon == null || entry.getValue() > mostCommonCount) {
                mostCommon = entry.getKey();
                mostCommonCount = entry.getValue();
            }
        }

        return mostCommon;
    }

    /**
     * PARA HACER calcula el nombre más común de todos los estudiantes que no están activos
     * en la clase utilizando streams paralelos. Debe reflejar la funcionalidad 
     * de mostCommonFirstNameOfInactiveStudentsImperative. Este método NO debe usar ciclos
     *
     * @param studentArray Datos de estudiantes para la clase.
     * @return Nombre más común de los estudiantes inactivos.
     */
    public String mostCommonFirstNameOfInactiveStudentsParallelStream(
            final Student[] studentArray) {
        // ----------------------------------------------------------------------------------------
        return Stream.of(studentArray) // Convierte el array en un stream secuencial
                .parallel()  // Le indica al framework fork/join de java a dividir el stream en subtareas y procesarlas en varios hilos según los núcleos paralelos. Parte el arreglo original en chunks y los distribuye entre los hilos que se encuentran en ForkJoinPool
                .filter(s -> !s.checkIsCurrent()) // Operación intermedia de tipo Lazy o perezosa, cada hilo toma estudiantes de su chunk y los descarta si están activos.
                .collect(Collectors.groupingBy( // Primer operación terminal del método, agrupa a los estudiantes inactivos por su primer nombre y cuantas veces aparece su nombre.
                        // Fase local: Cada hilo crea un HashMap local y a medida que lee su chunk va llenando su memoria local {"Juan":2, Maria: 1}
                        // Fase Join: El framework ForkJoin recolecta todos los mapas de todos los hilos y empieza a fusionarlos (merge),si hilo 1 {Juan:2} y hilo 2 {Juan:3} entonces {Juan:5}
                        Student::getFirstName, // Se obtiene el primer nombre.
                        Collectors.counting())) // Se hace el conteo de cada nombre

                .entrySet()  // Transforma el mapa resultante en un diccionario o set
                .stream() // Se crea un nuevo stream como no se especifica el método .parallel, entonces se deduce que es secuencial el proceso sobre el nuevo flujo de datos.
                .max(Map.Entry.comparingByValue())   // Segunda operación terminal, buscando la pareja que tenga el conteo más alto, consume el segundo stream y devuelve un objeto, que en este caso es un. Optional<Map. Entry<String, Long>>
                .map(Map.Entry::getKey)   // Es un método de la clase optional, si ya se encontró un ganador, lo que hace este es extraer la llave o nombre
                .orElse(null); // Si el arreglo original no tenía estudiantes inactivos, el mapa quedo vacío por lo que max no encontró nada y termina devolviendo null.
                // throw new UnsupportedOperationException();
        // ---------------------------------------------------------------------------------------- ADD #3

    }

    /**
     * Calcula secuencialmente el número de estudiantes que han perdido el curso
     * que son mayores de 20 años. Una calificación de perdido es cualquiera por debajo de 65 
     * 65. Un estudiante ha perdido el curso si tiene una calificación de perdido 
     * y no está activo en la actualidad
     *
     * @param studentArray Datos del estudiante para la clase.
     * @return Cantidad de calificaciones perdidas de estudiantes mayores de 20 años de edad.
     */
    public int countNumberOfFailedStudentsOlderThan20Imperative(
            final Student[] studentArray) {
        int count = 0;
        for (Student s : studentArray) {
            if (!s.checkIsCurrent() && s.getAge() > 20 && s.getGrade() < 65) {
                count++;
            }
        }
        return count;
    }

    /**
     * PARA HACER calcular el número de estudiantes que han perdido el curso 
     * que son mayores de 20 años de edad . una calificación de perdido está por debajo de 65. 
     * Un estudiante ha perdido el curso si tiene una calificación de perdido 
     * y no está activo en la actualidad. Debe reflejar la funcionalidad de
     * countNumberOfFailedStudentsOlderThan20Imperative. El método no debe usar ciclos.
	 *
     * @param studentArray Datos del estudiante para la clase.
     * @return Cantidad de calificaciones perdidas de estudiantes mayores de 20 años de edad.
     */
    public int countNumberOfFailedStudentsOlderThan20ParallelStream(
            final Student[] studentArray) {
        // -------------------------------------------------------------------------
        return (int) Stream.of(studentArray) // Convierte el array en un stream secuencial
                .parallel()  // Le indica al framework fork/join de java a dividir el stream en subtareas y procesarlas en varios hilos según los núcleos paralelos.
                .filter(
                        s -> !s.checkIsCurrent() &&
                                s.getAge() > 20 &&
                                s.getGrade() < 65) // Filtra los estudiantes inactivos
                .count(); // Contar elementos
        // throw new UnsupportedOperationException();
         // ------------------------------------------------------------------------- ADD #4

    }
}
