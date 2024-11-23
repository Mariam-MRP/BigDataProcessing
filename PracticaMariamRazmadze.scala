import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._



object examen {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  val spark=SparkSession.builder()
    .appName("EjerciciosEexamen")
    .master("local[*]")
    .config("spark.driver.bindAddress","0.0.0.0" )
    .config("spark.driver.host","0.0.0.0")
    .getOrCreate()
  import spark.implicits._
  val estudiantes = Seq(
    ("Juan", 30, 9.5),
    ("María", 22, 7.8),
    ("Pedro", 39, 8.0),
    ("Ana", 25, 5.1),
    ("Luis", 23, 6.5)
  ).toDF("nombre", "edad", "calificacion")


  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {
    estudiantes.printSchema()

    val estudiantesFiltrados = estudiantes.filter(col("calificacion") > 8)

    val estudiantesOrdenados = estudiantes
      .select("nombre", "calificacion")
      .orderBy(col("calificacion").desc)

    estudiantesOrdenados


  }

  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {

    val esPar: UserDefinedFunction = udf((numero: Int) => {
      if (numero % 2==0) "par" else "impar"
    })

    val dfPar = numeros.withColumn("par", esPar(col("numero")))

    dfPar

  }

  /**Ejercicio 3: Joins y agregaciones
  Pregunta: Dado dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  */
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {
    val spark=SparkSession.builder()
      .appName("EjerciciosEexamen")
      .master("local[*]")
      .config("spark.driver.bindAddress","0.0.0.0" )
      .config("spark.driver.host","0.0.0.0")
      .getOrCreate()
    import spark.implicits._

    val studentsData = Seq(
      (1, "Ana"),
      (2, "Roberto"),
      (3, "Carlos")
    )
    val studentsDF = studentsData.toDF("id", "nombre")


    val gradesData = Seq(
      (1, "Lengua", 8),
      (1, "Ingles", 9),
      (2, "Lengua", 7),
      (2, "Ingles", 8),
      (3, "Lengua", 4)
    )
    val gradesDF = gradesData.toDF("id_estudiante", "asignatura", "calificacion")


    val joinedDF = studentsDF
      .join(gradesDF, studentsDF("id") === gradesDF("id_estudiante"))


    val newDF = joinedDF
      .groupBy("id", "nombre")
      .agg(
        avg("calificacion").as("promedio_calificaciones")
      )

    newDF


  }

  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

  */

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {

    val rddPalabras = spark.sparkContext.parallelize(palabras)

    val contarPalabras = rddPalabras
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)

    contarPalabras


  }
  /**
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {
    val spark=SparkSession.builder()
      .appName("EjerciciosEexamen")
      .master("local[*]")
      .config("spark.driver.bindAddress","0.0.0.0" )
      .config("spark.driver.host","0.0.0.0")
      .getOrCreate()


    val df= spark.read
      .option("header", value=true)
      .csv("data/ventas.csv")

    val newDF = df.withColumn("ingresoTotal", col("cantidad") * col("precio_unitario"))


    newDF
      .groupBy("id_producto")
      .agg(functions.sum("ingresoTotal"))



  }

}
