package com.ecommerce.models

case class Transaction(
                        transaction_id: String,
                        user_id:        String,
                        product_id:     String,
                        merchant_id:    String,
                        amount:         Option[Double],
                        timestamp:      String,
                        location:       String,
                        payment_method: String,
                        category:       String

                      )
