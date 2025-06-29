require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const africastalking = require('africastalking')({
  apiKey: process.env.AT_API_KEY,
  username: process.env.AT_USERNAME,
});
const { Firestore } = require('@google-cloud/firestore');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// === === === === === === === === === ===
// ✅ Firestore setup
// === === === === === === === === === ===
const firestore = new Firestore({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GCP_KEY_FILE,
});

const txCollection = firestore.collection('transactions');

// === === === === === === === === === ===
// ✅ CORS setup
// === === === === === === === === === ===
const corsOptions = {
  origin: 'https://daima-pay-portal.onrender.com',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
// ✅ Ensure preflight requests are handled too
app.options('/*splat', cors(corsOptions));

// === === === === === === === === === ===
// ✅ Middlewares
// === === === === === === === === === ===
app.use(bodyParser.json());

// === === === === === === === === === ===
// ✅ POST /pay: Initiate M-PESA STK Push
// === === === === === === === === === ===
app.post('/pay', async (req, res) => {
  const { topupNumber, amount, mpesaNumber } = req.body;

  if (!topupNumber || !amount || !mpesaNumber) {
    return res.status(400).json({ error: 'Missing fields!' });
  }

  try {
    const auth = Buffer.from(
      `${process.env.DARAJA_CONSUMER_KEY}:${process.env.DARAJA_CONSUMER_SECRET}`
    ).toString('base64');

    const authResponse = await axios.get(
      'https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials',
      {
        headers: { Authorization: `Basic ${auth}` },
      }
    );

    const access_token = authResponse.data.access_token;

    const timestamp = new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 14);
    const password = Buffer.from(
      `${process.env.BUSINESS_SHORTCODE}${process.env.DARAJA_PASSKEY}${timestamp}`
    ).toString('base64');

    const stkPushPayload = {
      BusinessShortCode: process.env.BUSINESS_SHORTCODE,
      Password: password,
      Timestamp: timestamp,
      TransactionType: 'CustomerPayBillOnline',
      Amount: amount,
      PartyA: mpesaNumber,
      PartyB: process.env.BUSINESS_SHORTCODE,
      PhoneNumber: mpesaNumber,
      CallBackURL: `${process.env.BASE_URL}/stk-callback`,
      AccountReference: 'DaimaPay',
      TransactionDesc: 'Airtime Purchase',
    };

    const stkResponse = await axios.post(
      'https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest',
      stkPushPayload,
      {
        headers: {
          Authorization: `Bearer ${access_token}`,
        },
      }
    );

    console.log('✅ STK Push Response:', stkResponse.data);

    await txCollection.doc(stkResponse.data.CheckoutRequestID).set({
      topupNumber: topupNumber,
      amount: amount,
      payer: mpesaNumber,
      status: 'PENDING',
      createdAt: new Date().toISOString(),
    });

    res.json({
      message: 'STK push initiated. Check your phone!',
      CheckoutRequestID: stkResponse.data.CheckoutRequestID,
      MerchantRequestID: stkResponse.data.MerchantRequestID,
      transID: stkResponse.data.CheckoutRequestID,
    });

  } catch (err) {
    console.error(err.response ? err.response.data : err);
    res.status(500).json({ error: 'STK push failed' });
  }
});

// === === === === === === === === === ===
// ✅ POST /stk-callback: Safaricom Callback
// === === === === === === === === === ===
app.post('/stk-callback', async (req, res) => {
  const callback = req.body;

  console.log('📞 Received STK Callback:', JSON.stringify(callback));

  const resultCode = callback.Body.stkCallback.ResultCode;
  const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID;

  if (resultCode === 0) {
    const metadata = callback.Body.stkCallback.CallbackMetadata;
    const amount = metadata.Item.find(i => i.Name === 'Amount').Value;

    const txDoc = await txCollection.doc(checkoutRequestID).get();

    if (!txDoc.exists) {
      console.error('❌ No matching transaction found for:', checkoutRequestID);
      return res.json({ resultCode: 0, resultDesc: 'No matching transaction' });
    }

    const txData = txDoc.data();
    const topupNumber = txData.topupNumber;

    console.log('✅ Found transaction. Sending airtime to:', topupNumber);

    try {
      const response = await africastalking.AIRTIME.send({
        recipients: [{ phoneNumber: topupNumber, amount: `KES ${amount}` }],
      });

      console.log('✅ Airtime sent:', response);

      await txCollection.doc(checkoutRequestID).update({
        status: 'COMPLETED',
        completedAt: new Date().toISOString(),
      });

    } catch (err) {
      console.error('❌ Airtime send failed:', err);
      await txCollection.doc(checkoutRequestID).update({
        status: 'FAILED_AIRTIME',
        error: err.toString(),
      });
    }
  } else {
    console.log('❌ Payment failed or cancelled.');

    await txCollection.doc(checkoutRequestID).update({
      status: 'FAILED_PAYMENT',
    });
  }

  res.json({ resultCode: 0, resultDesc: 'Received' });
});

// === === === === === === === === === ===
// ✅ Health check
// === === === === === === === === === ===
app.get('/', (req, res) => {
  res.send('DaimaPay backend is running ✅');
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
