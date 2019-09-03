package tfm.DataPreparation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace, udf}
import tfm.{NLPProcesor, config}

import scala.util.matching.Regex

case class Slang(Slang: String, Meaning: String)

class Cleaning extends Serializable {
  def clean(df: DataFrame): DataFrame = {
    val column = "Queries"
    val charList = List('ç', 'ñ')
    val wordList = List("good","bad")
    val expressionList = List("good bye","bad", "friend")
    val aditionalTokens = List("&bnsp;")

    val dfFinal = df
      .transform(apostropheCleaning(column))
      .transform(attachedWordsCleaning(column))
      .transform(charactersCleaning(column, charList))
      .transform(contentCleaning(column,wordList))
      .transform(decodingCleaning(column))
      .transform(expressionsCleaning(column,expressionList))
      .transform(htmlCleaning(column, aditionalTokens))
      .transform(initialFinalApostropheCleaning(column))
      .transform(slangCleaning(column))
      .transform(urlCleaning(column, "OFF"))
    dfFinal
  }

  /**
   * Eliminación de apostrofe. I'd like to ... -->  I would like to
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def apostropheCleaning(column: String)(df: DataFrame): DataFrame = {
    val model = new NLPProcesor(column)
    model.transform(df)
  }


  /**
   * Separación de palabras con mayúsculas. GoodBye --> Good Bye
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def attachedWordsCleaning(column: String)(df: DataFrame): DataFrame = {
    import scala.util.matching.Regex
    val regexCamelCase = "[A-Z][^A-Z]+"
    val keyValPattern: Regex = regexCamelCase.r

    val attachedWordsCleaningUDF = udf { s: String =>
      s.split(" ").map(
        x => {
          if (keyValPattern.findAllMatchIn(x).isEmpty)
            x
          else
            keyValPattern.findAllMatchIn(x).mkString(" ")
        }).mkString(" ")
    }
    df.withColumn(column, attachedWordsCleaningUDF(col(column)))
  }


  /**
   * Eliminación de caracteres definidos por usuario como ç o ñ. çhola --> hola
   *
   * @param df       DataFrame con los datos de entrada.
   * @param column   Columna en la que hay que realizar el proceso.
   * @param charList Lista de caracteres que se desean eliminar de la columna
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def charactersCleaning(column: String, charList: List[Char])(df: DataFrame): DataFrame = {
    /** FoldLeft sirve para aplicar una operación a los elementos de una colección dando el valor inicial que se indique.
     * En este caso para la lista de caracteres List(ç,ñ) lo que hace es poner el paréntesis al principio y luego aplica
     * la operación "+" a todos los elementos, es decir concatenar los caracteres de la lista quedando "(çñ" una vez hecha
     * la operación se le añade + ")" para que quede la expresión regular (ç|ñ) que servirá para eliminar estos caracteres
     */
    val regExpIntern = charList.foldLeft("")(_ + '|' + _)
    val regExp = "(" + regExpIntern.subSequence(1, regExpIntern.length) + ")"
    df.withColumn(column, regexp_replace(col(column), regExp, ""))
  }

  /**
   *
   * Eliminación de palabras definidas por usuario.
   *
   * @param df       DataFrame con los datos de entrada.
   * @param column   Columna en la que hay que realizar el proceso.
   * @param wordList Lista de palabras que se desean eliminar de la columna
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def contentCleaning(column: String, wordList: List[String])(df: DataFrame): DataFrame = {
    import scala.util.matching.Regex

    val regExpIntern = wordList.foldLeft("")(_ + '|' + _)
    val regExp = "^(" + regExpIntern.subSequence(1, regExpIntern.length) + ")$"
    val keyValPattern: Regex = regExp.r

    val contentCleaningUDF = udf { s: String =>
      s.split(" ").map(
        x => keyValPattern.replaceAllIn(x, "")
      ).mkString(" ")
    }

    df.withColumn(column, contentCleaningUDF(col(column)))
  }

  /**
   * Eliminación de caracteres no ASCII
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def decodingCleaning(column: String)(df: DataFrame): DataFrame = {
    df.withColumn(column, regexp_replace(col(column), "[^\\x00-\\x7F]", ""))
  }

  /**
   * Eliminacion de expresiones. Eliminar "good bye". good bye my friend --> my friend
   *
   * @param df            DataFrame con los datos de entrada.
   * @param column        Columna en la que hay que realizar el proceso.
   * @param expresionList Lista de expresiones que se desean eliminar de la columna indicada
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def expressionsCleaning(column: String, expresionList: List[String])(df: DataFrame): DataFrame = {

    val regExpIntern = expresionList.foldLeft("")(_ + ")|(" + _)
    val regExp = regExpIntern.subSequence(2, regExpIntern.length) + ")"
    df.withColumn(column, regexp_replace(col(column), regExp, ""))
  }


  /**
   * Extración de texto de HTML. "<html>&bnsp;hola</html>" --> "hola"
   *
   * @param df              DataFrame con los datos de entrada.
   * @param column          Columna en la que hay que realizar el proceso.
   * @param aditionalTokens Lista de elementos que se desean eliminar de la columna indicada
   * @return El DataFrame con los datos modificados en la columna indicada
   *
   */
  def htmlCleaning(column: String, aditionalTokens: List[String])(df: DataFrame): DataFrame = {
    val regExpIntern = aditionalTokens.foldLeft("")(_ + ")|(" + _)
    val regExp = regExpIntern.subSequence(2, regExpIntern.length) + ")"
    val regexHtmlTags = "(<.*?>)"
    val regexFinal = s"$regExp|$regexHtmlTags"

    df.withColumn(column, regexp_replace(col(column), regexFinal, ""))

  }


  /**
   * Eliminacion de comillas en el primer y ultimo carácter. 'Hola' --> Hola
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def initialFinalApostropheCleaning(column: String)(df: DataFrame): DataFrame = {
    val initialFinalRegExp = "(^'|'$)"
    df.withColumn(column, regexp_replace(col(column), initialFinalRegExp, ""))
  }

  /**
   * Traducción de siglas o abreviaturas. "AFAIK" --> "As Far As I Know"
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def slangCleaning(column: String)(df: DataFrame): DataFrame = {
    val path = "resources/slang_dict.csv"
    import config.Config.spark.implicits._
    val slangDict: Array[Slang] = config.Config.spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "`")
      .load(path)
      .as[Slang]
      .collect()

    val slangCleaningUdf = udf { value: String =>
      val tokenList = value.split(" ")
      val processedTokens = tokenList.map(x => {
        val filtered = slangDict.filter(elem => elem.Slang == x)
        if (filtered.length > 0)
          filtered(0).Meaning
        else
          x
      })
      val outputString = processedTokens.mkString(" ")
      outputString
    }

    df.withColumn(column, slangCleaningUdf(col(column)))
  }

  /**
   * Limpieza de URL. www.google.com --> Google, www.google.com --> [None]
   *
   * @param df     DataFrame con los datos de entrada.
   * @param column Columna en la que hay que realizar el proceso.
   * @param keepDomain OFF: Elimina la URL completa; ON: Deja el nombre del dominio; CAPITALIZED: Deja el nombre del
   *                   dominio con la primera letra en mayúscula
   * @return El DataFrame con los datos modificados en la columna indicada
   */
  def urlCleaning(column: String, keepDomain: String)(df: DataFrame): DataFrame = {
    import scala.util.matching.Regex

    val regExp = "^(http:\\/\\/www\\.|site:|https:\\/\\/www\\.|http:\\/\\/|https:\\/\\/)?[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?(\\/.*)?$"
    val keyValPattern: Regex = regExp.r


    val urlCleaningUDF = udf { s: String =>
      if (keepDomain == "OFF") {
        remove_urls(s, keyValPattern)
      }
      else {
        remove_urls_keep_domain(s,keepDomain, keyValPattern)
      }
    }

    df.withColumn(column, urlCleaningUDF(col(column)))
  }

  /**
   * Elimina una URL de una cadena de texto
   *
   * @param input_data Cadena de texto de entrada
   * @param urlRegex Expresión regular que detecta una URL
   * @return Cadena de texto con la url eliminada
   */
  private def remove_urls(input_data: String, urlRegex: Regex): String = {
    val value = input_data.split(" ").map(x => {
      urlRegex.replaceAllIn(x, "")
    }).mkString(" ")
    value
  }

  /**
   * Elimina una URL de una cadena de texto manteniendo el dominio
   * EJ: www.google.com --> google
   *
   *
   * @param input_data Cadena de texto de entrada
   * @param urlRegex Expresión regular que detecta una URL
   * @return Cadena de texto con el dominio
   */
  private def remove_urls_keep_domain(input_data: String,keepDomain: String, urlRegex: Regex): String = {
    input_data.split(" ").map(x => remove_urls_keep_domain_in_token(x, keepDomain, urlRegex)).mkString(" ")
  }


  /**
   *
   * @param token      Cadena de texto de entrada
   * @param keepDomain OFF: Elimina la URL completa; ON: Deja el nombre del dominio; CAPITALIZED: Deja el nombre del
   *                   dominio con la primera letra en mayúscula
   * @param urlRegex Expresión regular de una URL
   * @return Cadena de texto con el dominio
   */
  private def remove_urls_keep_domain_in_token(token: String,keepDomain:String,  urlRegex: Regex): String = {
    import java.net.URI
    if (!urlRegex.findAllIn(token).isEmpty) {
      val url = new URI(token)
      val hostname = url.getHost
      val hostname_tokens =
        if (hostname == null)
          url.getPath.split("\\.")
        else
          hostname.split("\\.")

      val domain: String =
        if (hostname_tokens.length < 3)
          hostname_tokens(0)
        else {
          if (hostname_tokens(hostname_tokens.length-2) == "co")
            hostname_tokens(hostname_tokens.length-3)
          else
            hostname_tokens(hostname_tokens.length-2)
        }
      if (keepDomain == "CAPITALIZED")
        domain.capitalize
      else {
        println(s"original: ${token}, domain: ${domain}")
        domain
      }
    }
    else
      token
  }

}
