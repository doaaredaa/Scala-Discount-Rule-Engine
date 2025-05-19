package MyProject

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.logging.{FileHandler, Level, Logger, SimpleFormatter}
import scala.io.Source
import scala.util.{Try, Success, Failure}

object MyProject extends App {
  // Set up logging
  private val logger: Logger = {
    val log = Logger.getLogger("RuleEngine")
    val fileHandler = new FileHandler("rules_engine.log")
    fileHandler.setFormatter(new SimpleFormatter())
    log.addHandler(fileHandler)
    log.setLevel(Level.ALL)
    log
  }

  private def logMessage(level: Level, message: String): Unit = {
    val timestamp = LocalDateTime.now().toString
    logger.log(level, s"$timestamp   ${level.getName}   $message")
  }

  // Read input file
  val lines: List[String] = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail

  case class Record(
                     timestamp: String,
                     product_name: String,
                     expiry_date: String,
                     quantity: Int,
                     unit_price: Double,
                     channel: String,
                     payment_method: String,
                     discount: Double = 0.0,
                     final_price: Double = 0.0
                   )

  def toRecord(line: String): Record = {
    val part = line.split(',')
    Record(
      timestamp = part(0),
      product_name = part(1),
      expiry_date = part(2),
      quantity = part(3).toInt,
      unit_price = part(4).toDouble,
      channel = part(5),
      payment_method = part(6)
    )
  }

  // Date formatter for parsing dates
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  //........................................................
  // Your existing discount rules
  def cheeseQ(x: Record): Boolean =
    x.product_name.toLowerCase.startsWith("cheese") ||
      x.product_name.toLowerCase.startsWith("wine")

  def cheese(x: Record): Double = x.product_name.split(" - ").head match {
    case "Cheese" => 10.0
    case "Wine" => 5.0
    case _ => 0.0
  }

  //........................................................
  def visaQ(y: Record): Boolean = y.payment_method.equals("Visa")

  def visa(y: Record): Double = 5.0

  //........................................................
  def more5Q(z: Record): Boolean = z.quantity > 5

  def more5(z: Record): Double = z.quantity match {
    case 6 | 7 | 8 | 9 => 5.0
    case 10 | 11 | 12 | 13 | 14 => 7.0
    case x => if (x >= 15) 10.0 else 0.0
  }

  //........................................................
  // New discount rules for the project requirements
  def expiryDiscountQ(r: Record): Boolean = {
    Try {
      val expiryDate = LocalDate.parse(r.expiry_date, dateFormatter)
      val transactionDate = LocalDate.parse(r.timestamp.split("T").head, dateFormatter)
      val daysRemaining = expiryDate.toEpochDay - transactionDate.toEpochDay
      daysRemaining < 30 && daysRemaining > 0
    }.getOrElse(false)
  }

  def expiryDiscount(r: Record): Double = {
    Try {
      val expiryDate = LocalDate.parse(r.expiry_date, dateFormatter)
      val transactionDate = LocalDate.parse(r.timestamp.split("T").head, dateFormatter)
      val daysRemaining = expiryDate.toEpochDay - transactionDate.toEpochDay
      30 - daysRemaining.toDouble
    }.getOrElse(0.0)
  }

  def march23DiscountQ(r: Record): Boolean = {
    Try {
      val transactionDate = r.timestamp.split("T").head
      transactionDate.matches(".*-03-23")
    }.getOrElse(false)
  }

  def march23Discount(r: Record): Double = 50.0

  // Combine all discount rules
  def getAllDiscountRules: List[(Record => Boolean, Record => Double, String)] = List(
    (expiryDiscountQ, expiryDiscount, "ExpiryDiscount"),
    (cheeseQ, cheese, "CheeseWineDiscount"),
    (visaQ, visa, "VisaDiscount"),
    (more5Q, more5, "QuantityDiscount"),
    (march23DiscountQ, march23Discount, "March23SpecialDiscount")
  )

  // Modified discount function to include logging and new rules
  def discount(h: Record): Record = {
    val rules = getAllDiscountRules
    val applicableDiscounts = rules
      .filter { case (qualify, _, name) =>
        val applies = qualify(h)
        if (applies) logMessage(Level.INFO, s"Rule $name qualifies for product ${h.product_name}")
        applies
      }
      .map { case (_, calculate, name) =>
        val discountValue = calculate(h)
        logMessage(Level.INFO, s"Applying ${discountValue}% discount from rule $name for product ${h.product_name}")
        discountValue
      }

    val dis = if (applicableDiscounts.nonEmpty) {
      val topTwo = applicableDiscounts.sorted(Ordering[Double].reverse).take(2)
      val avg = topTwo.sum / topTwo.length
      logMessage(Level.INFO, s"Calculated average of top 2 discounts: $avg% for product ${h.product_name}")
      avg
    } else {
      logMessage(Level.INFO, s"No discounts apply for product ${h.product_name}")
      0.0
    }

    val finalPrice = h.unit_price * h.quantity * (1 - dis / 100)
    logMessage(Level.INFO, s"Final price for product ${h.product_name}: $finalPrice")

    h.copy(
      discount = dis,
      final_price = finalPrice
    )
  }

  // Database simulation
  def saveToDatabase(record: Record): Try[Unit] = Try {
    logMessage(Level.INFO, s"Saving to database: ${record.product_name}, Discount: ${record.discount}%, Final Price: ${record.final_price}")
    Thread.sleep(100)
  }

  // Process all records
  val processedRecords = lines.map(toRecord).map(discount)

  // Save to database and handle errors
  processedRecords.foreach { record =>
    saveToDatabase(record) match {
      case Success(_) => logMessage(Level.INFO, s"Successfully saved record for ${record.product_name}")
      case Failure(e) => logMessage(Level.SEVERE, s"Failed to save record for ${record.product_name}: ${e.getMessage}")
    }
  }

  // Print first 10 records for demonstration
  processedRecords.take(10).foreach(println)
}