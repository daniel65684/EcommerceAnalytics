package com.ecommerce.models

case class User(
                 user_id:              String,
                 age:                  Option[Int],
                 annual_income:        Option[Double],
                 city:                 String,
                 customer_segment:     String,
                 preferred_categories: Seq[String],
                 registration_date:    String
               )
