// server.js - For STK Push Initiation and M-Pesa STK Callback Handling (NO OPTIONAL CHAINING)

// --- IMPORTS AND CONFIGURATION ---
require('dotenv').config(); // Load environment variables from .env file
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');
const { FieldValue } = require('firebase-admin/firestore');
const rateLimit = require('express-rate-limit');
const winston = require('winston'); // For logging
const cors = require('cors'); // Added CORS

// Initialize Firebase Admin SDk
const serviceAccount = JSON.parse(
    Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_JSON, 'base64').toString('utf-8')
);

// Check if a Firebase app has already been initialized to avoid re-initialization errors
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount) // Use admin.credential.cert() with the parsed serviceAccount
    });
}

const firestore = admin.firestore();

// Firestore Collection References
const transactionsCollection = firestore.collection('transactions');
const salesCollection = firestore.collection('sales'); // This will store initial requests AND fulfillment details
const errorsCollection = firestore.collection('errors');
const failedReconciliationsCollection = firestore.collection('failed_reconciliations');
const reconciledTransactionsCollection = firestore.collection('reconciled_transactions');
const airtimeBonusesCollection = firestore.collection('airtime_bonuses'); // For bonus settings
const carrierFloatsCollection = firestore.collection('carrier_floats'); // For float balances

// M-Pesa API Credentials from .env
const CONSUMER_KEY = process.env.DARAPAY_CONSUMER_KEY;
const CONSUMER_SECRET = process.env.DARAPAY_CONSUMER_SECRET;
const SHORTCODE = process.env.DARAPAY_SHORTCODE; // Your Paybill/Till number
const PASSKEY = process.env.DARAPAY_PASSKEY;
const STK_CALLBACK_URL = process.env.DARAPAY_STK_CALLBACK_URL; // Your public URL for /stk-callback
const ANALYTICS_SERVER_URL = process.env.ANALYTICS_SERVER_URL; // Your analytics server URL

// Logger setup
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console(),
        // new winston.transports.File({ filename: 'error.log', level: 'error' }),
        // new winston.transports.File({ filename: 'combined.log' })
    ],
});

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// --- CORS Configuration ---
// Allow specific origins (recommended for production)
const allowedOrigins = [
     'https://daima-pay-portal.onrender.com',
  'https://dpanalyticsserver.onrender.com'
];
app.use(cors({
    origin: function (origin, callback) {
        // allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Allowed HTTP methods
    credentials: true, // Allow cookies to be sent with requests (if needed)
    optionsSuccessStatus: 204 // Some legacy browsers (IE11, various SmartTVs) choke on 200
}));


// --- HELPER FUNCTIONS (PLACEHOLDERS - Implement these based on your existing code) ---

// Function to get Daraja access token
async function getAccessToken() {
    const auth = Buffer.from(`${CONSUMER_KEY}:${CONSUMER_SECRET}`).toString('base64');
    try {
        const response = await axios.get('https://api.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials', {
            headers: {
                'Authorization': `Basic ${auth}`
            }
        });
        return response.data.access_token;
    } catch (error) {
        logger.error('Error getting access token:', error.message);
        throw new Error('Failed to get M-Pesa access token.');
    }
}

// Function to generate the timestamp for M-Pesa API
function generateTimestamp() {
    const date = new Date();
    const year = date.getFullYear().toString();
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const day = date.getDate().toString().padStart(2, '0');
    const hour = date.getHours().toString().padStart(2, '0');
    const minute = date.getMinutes().toString().padStart(2, '0');
    const second = date.getSeconds().toString().padStart(2, '0');
    return `${year}${month}${day}${hour}${minute}${second}`;
}

// Function to generate password for STK Push
function generatePassword(shortcode, passkey, timestamp) {
    const str = shortcode + passkey + timestamp;
    return Buffer.from(str).toString('base64');
}

// Placeholder for detecting carrier (You already have this in your C2B handler)
function detectCarrier(phoneNumber) {
    const normalized = phoneNumber.replace(/^(\+254|254)/, '0').trim();
    if (normalized.length !== 10 || !normalized.startsWith('0')) {
        logger.debug(`Invalid phone number format for carrier detection: ${phoneNumber}`);
        return 'Unknown';
    }
    const prefix3 = normalized.substring(1, 4);

    const safaricom = new Set([
        '110', '111', '112', '113', '114', '115', '116', '117', '118', '119',
        '700', '701', '702', '703', '704', '705', '706', '707', '708', '709',
        '710', '711', '712', '713', '714', '715', '716', '717', '718', '719',
        '720', '721', '722', '723', '724', '725', '726', '727', '728', '729',
        '740', '741', '742', '743', '744', '745', '746', '748', '749',
        '757', '758', '759',
        '768', '769',
        '790', '791', '792', '793', '794', '795', '796', '797', '798', '799'
    ]);
    const airtel = new Set([
        '100', '101', '102', '103', '104', '105', '106', '107', '108', '109',
        '730', '731', '732', '733', '734', '735', '736', '737', '738', '739',
        '750', '751', '752', '753', '754', '755', '756',
        '780', '781', '782', '783', '784', '785', '786', '787', '788', '789'
    ]);
    const telkom = new Set([
        '770', '771', '772', '773', '774', '775', '776', '777', '778', '779'
    ]);
    const equitel = new Set([
        '764', '765', '766', '767',
    ]);
    const faiba = new Set([
        '747',
    ]);

    if (safaricom.has(prefix3)) return 'Safaricom';
    if (airtel.has(prefix3)) return 'Airtel';
    if (telkom.has(prefix3)) return 'Telkom';
    if (equitel.has(prefix3)) return 'Equitel';
    if (faiba.has(prefix3)) return 'Faiba';
    return 'Unknown';
}

// Placeholder for updating carrier float balance on your analytics/float management server
// This is called by the STK Callback to deduct the amount received from the customer.
// The fulfillment process will handle the actual float deductions for airtime dispatch.
async function updateCarrierFloatBalance(floatName, amount) {
    logger.info(`Attempting to update float balance for ${floatName} by ${amount}`);
    try {
        // This should hit your Analytics Server's endpoint for float adjustments
        const response = await axios.post(`${ANALYTICS_SERVER_URL}/api/update-float`, {
            floatName: floatName,
            amount: amount
        });
        logger.info(`Float update response for ${floatName}:`, response.data);
        return { success: true, data: response.data };
    } catch (error) {
        logger.error(`Error updating float balance for ${floatName}:`, error.message);
        return { success: false, message: error.message };
    }
}

// --- RATE LIMITING ---
const stkPushLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 20, // Limit each IP to 20 requests per window
    message: 'Too many STK Push requests from this IP, please try again after a minute.',
    statusCode: 429,
    headers: true,
});

const stkCallbackRateLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 100, // M-Pesa can send multiple retries
    message: 'Too many STK Callback requests, please try again later.',
    statusCode: 429,
    headers: true,
});


// --- ENDPOINTS ---

// 1. STK Push Initiation Endpoint
app.post('/stk-push', stkPushLimiter, async (req, res) => {
    const { amount, phoneNumber, recipient } = req.body; // recipient is the number to top up

    if (!amount || !phoneNumber || !recipient) {
        return res.status(400).json({ success: false, message: 'Missing required parameters: amount, phoneNumber, recipient.' });
    }

    const timestamp = generateTimestamp();
    const password = generatePassword(SHORTCODE, PASSKEY, timestamp);
    const checkoutRequestID = `STK-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`; // Unique ID for our system

    logger.info(`Initiating STK Push for recipient: ${recipient}, amount: ${amount}, customer: ${phoneNumber}`);

    try {
        const accessToken = await getAccessToken();
        const detectedCarrier = detectCarrier(recipient); // Detect carrier at initiation

        // --- Create initial request document in salesCollection ---
        // This document will be updated by the STK callback
        // and later by the fulfillment process.
        await salesCollection.doc(checkoutRequestID).set({
            saleId: checkoutRequestID, // Use checkoutRequestID as saleId for this stage
            initiatorPhoneNumber: phoneNumber, // The phone number initiating the STK push
            recipient: recipient, // The number to top up
            amount: parseFloat(amount), // Original amount requested by customer
            carrier: detectedCarrier, // Detected carrier for the recipient
            mpesaPaymentStatus: 'PENDING_MPESA_CONFIRMATION', // Initial status
            createdAt: FieldValue.serverTimestamp(),
            lastUpdated: FieldValue.serverTimestamp(),
            type: 'STK_PUSH_REQUEST', // Distinguish from fulfilled sales
            // Other fields will be added by the callback and fulfillment logic
        });
        logger.info(`✅ Initial sale request document ${checkoutRequestID} created.`);


        const stkPushResponse = await axios.post('https://api.safaricom.co.ke/mpesa/stkpush/v1/processrequest', {
            BusinessShortCode: SHORTCODE,
            Password: password,
            Timestamp: timestamp,
            TransactionType: 'CustomerPayBillOnline', // Or 'CustomerBuyGoodsOnline' if applicable
            Amount: amount,
            PartyA: phoneNumber, // Customer's phone number
            PartyB: SHORTCODE, // Your Paybill/Till number
            PhoneNumber: phoneNumber, // Customer's phone number
            CallBackURL: STK_CALLBACK_URL,
            AccountReference: recipient, // Use recipient number as account reference
            TransactionDesc: `Airtime for ${recipient}`
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`
            }
        });

        logger.info('STK Push Request Sent:', stkPushResponse.data);

        // Update the initial request document with M-Pesa response
        await salesCollection.doc(checkoutRequestID).update({
            mpesaResponse: stkPushResponse.data,
            lastUpdated: FieldValue.serverTimestamp()
        });
        logger.info(`✅ Initial sale request document ${checkoutRequestID} updated with STK Push response.`);

        res.status(200).json({ success: true, message: 'STK Push initiated successfully.', data: stkPushResponse.data });

    } catch (error) {
        logger.error('❌ Error during STK Push initiation:', {
            message: error.message,
            stack: error.stack,
            requestBody: req.body
        });

        const errorMessage = error.response ? error.response.data : error.message;

        // Log the error
        await errorsCollection.add({
            type: 'STK_PUSH_INITIATION_ERROR',
            error: errorMessage,
            requestBody: req.body,
            createdAt: FieldValue.serverTimestamp(),
        });

        res.status(500).json({ success: false, message: 'Failed to initiate STK Push.', error: errorMessage });
    }
});


// 2. M-Pesa STK Callback Endpoint (where M-Pesa sends payment confirmation)
app.post('/stk-callback', stkCallbackRateLimiter, async (req, res) => {
    const callback = req.body;
    const now = FieldValue.serverTimestamp();

    logger.info('📞 Received STK Callback:', JSON.stringify(callback));

    const resultCode = callback.Body.stkCallback.ResultCode;
    const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID; // This will be the transactionID for consistency

    // --- REPLACED OPTIONAL CHAINING HERE ---
    let mpesaReceiptNumber = null;
    let transactionDateFromMpesa = null;
    let phoneNumberUsedForPayment = null;
    let amountPaidByCustomer = null;

    const callbackMetadata = callback.Body.stkCallback.CallbackMetadata;
    if (callbackMetadata && Array.isArray(callbackMetadata.Item)) {
        const receiptItem = callbackMetadata.Item.find(item => item.Name === 'MpesaReceiptNumber');
        if (receiptItem) {
            mpesaReceiptNumber = receiptItem.Value;
        }

        const dateItem = callbackMetadata.Item.find(item => item.Name === 'TransactionDate');
        if (dateItem) {
            transactionDateFromMpesa = dateItem.Value;
        }

        const phoneItem = callbackMetadata.Item.find(item => item.Name === 'PhoneNumber');
        if (phoneItem) {
            phoneNumberUsedForPayment = phoneItem.Value;
        }

        const amountItem = callbackMetadata.Item.find(item => item.Name === 'Amount');
        if (amountItem) {
            amountPaidByCustomer = amountItem.Value;
        }
    }
    // --- END REPLACED OPTIONAL CHAINING ---

    // Retrieve the initial sales request document
    const initialSalesRequestDocRef = salesCollection.doc(checkoutRequestID);
    const initialSalesRequestDoc = await initialSalesRequestDocRef.get();

    if (!initialSalesRequestDoc.exists) {
        logger.error('❌ No matching initial sales request for CheckoutRequestID in Firestore:', checkoutRequestID);
        await errorsCollection.doc(`STK_CALLBACK_NO_INITIAL_REQUEST_${checkoutRequestID}_${Date.now()}`).set({
            type: 'STK_CALLBACK_ERROR',
            error: 'No matching initial sales request found for CheckoutRequestID. Cannot process callback.',
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });
        return res.json({ ResultCode: 0, ResultDesc: 'Callback received, but initial transaction request not found locally.' });
    }

    const initialRequestData = initialSalesRequestDoc.data();
    const topupNumber = initialRequestData.recipient; // Get recipient from initial request
    const carrier = initialRequestData.carrier; // Get carrier from initial request

    let finalTransactionStatus; // Reflects payment outcome and initial fulfillment state
    let needsReconciliation = false; // For payment processing errors (e.g., analytics update failed)

    // Reference to the 'transactions' document (using checkoutRequestID as its ID)
    const transactionDocRef = transactionsCollection.doc(checkoutRequestID);

    if (resultCode === 0) {
        // M-Pesa payment was successful
        logger.info(`✅ M-Pesa payment successful for ${checkoutRequestID}. Marking for fulfillment.`);
        finalTransactionStatus = 'RECEIVED_PENDING_FULFILLMENT'; // Unified status

        // Create or update the transaction record with initial details
        await transactionDocRef.set({
            transactionID: checkoutRequestID, // M-Pesa's unique ID for this transaction
            type: 'STK_PUSH_PAYMENT', // Explicitly mark as STK Push
            transactionTime: transactionDateFromMpesa, // M-Pesa's timestamp
            amountReceived: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null, // Actual amount paid
            mpesaReceiptNumber: mpesaReceiptNumber, // M-Pesa receipt
            payerMsisdn: phoneNumberUsedForPayment,
            // payerName: initialRequestData.customerName || null, // If you capture this at initiation
            billRefNumber: topupNumber, // The recipient number for airtime
            carrier: carrier, // The carrier for the recipient
            mpesaRawCallback: callback, // Full M-Pesa callback data
            status: finalTransactionStatus, // Set to pending fulfillment
            fulfillmentStatus: 'PENDING', // Initial fulfillment status
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            createdAt: initialRequestData.createdAt || now, // Use initial request timestamp if available
            lastUpdated: now,
            relatedSaleId: checkoutRequestID, // Link to the existing sales request doc
            reconciliationNeeded: false, // Assume not needed initially for payment processing
        }, { merge: true }); // Use merge to avoid overwriting if doc already exists from initial request logging
        logger.info(`✅ [transactions] Record for ${checkoutRequestID} created/updated with status: ${finalTransactionStatus}`);

        // --- Notify Analytics Server for float deduction (customer's payment received) ---
        try {
            logger.info(`Attempting to notify Analytics Server for payment ${checkoutRequestID} (type: PAYMENT_RECEIVED)...`);
            const analyticsPayload = {
                transactionId: checkoutRequestID,
                amount: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : 0, // Amount customer paid
                status: 'PAYMENT_RECEIVED', // Report only payment receipt
                carrier: carrier, // The carrier for the airtime recipient
                type: 'STK_PUSH_PAYMENT',
                mpesaReceiptNumber: mpesaReceiptNumber,
                payerMsisdn: phoneNumberUsedForPayment,
                // fulfillment-related fields are not set here:
                bonusAmount: 0,
                commissionRate: 0,
                amountDispatched: 0,
                providerUsed: null
            };
            const analyticsResponse = await axios.post(`${ANALYTICS_SERVER_URL}/api/process-airtime-purchase`, analyticsPayload);
            logger.info(`✅ Analytics Server response for float adjustment (payment received):`, analyticsResponse.data);

        } catch (deductionError) {
            logger.error(`❌ Failed to call Analytics Server for float adjustment (payment received) for ${checkoutRequestID}:`, deductionError.message);
            await errorsCollection.doc(`ANALYTICS_PAYMENT_NOTIFY_FAIL_${checkoutRequestID}`).set({
                type: 'ANALYTICS_NOTIFICATION_ERROR',
                subType: 'STK_PUSH_PAYMENT_RECEIVED',
                error: `Failed to notify Analytics Server for payment received: ${deductionError.message}`,
                transactionId: checkoutRequestID,
                carrier: carrier,
                amount: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : 0,
                statusReported: 'PAYMENT_RECEIVED',
                stack: deductionError.stack,
                createdAt: now,
            });
            // Mark for reconciliation as analytics update failed
            needsReconciliation = true;
            await transactionDocRef.update({
                reconciliationNeeded: true,
                errorMessage: `Analytics notification for payment receipt failed: ${deductionError.message}`
            });
            logger.warn(`⚠️ Transaction ${checkoutRequestID} marked for reconciliation due to analytics notification failure.`);
        }

    } else {
        // M-Pesa payment failed or was cancelled by user
        logger.info(`❌ Payment failed for ${checkoutRequestID}. ResultCode: ${resultCode}, Desc: ${callback.Body.stkCallback.ResultDesc}`);
        finalTransactionStatus = 'MPESA_PAYMENT_FAILED';

        await transactionDocRef.set({
            transactionID: checkoutRequestID,
            type: 'STK_PUSH_PAYMENT',
            transactionTime: transactionDateFromMpesa,
            amountReceived: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
            mpesaReceiptNumber: mpesaReceiptNumber,
            payerMsisdn: phoneNumberUsedForPayment,
            billRefNumber: topupNumber,
            carrier: carrier,
            mpesaRawCallback: callback,
            status: finalTransactionStatus, // Payment failed
            fulfillmentStatus: 'NOT_APPLICABLE', // No fulfillment needed
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            errorMessage: `M-Pesa payment failed: ${callback.Body.stkCallback.ResultDesc}`,
            createdAt: initialRequestData.createdAt || now,
            lastUpdated: now,
            relatedSaleId: checkoutRequestID,
            reconciliationNeeded: false, // No reconciliation needed as customer didn't pay
        }, { merge: true });
        logger.info(`✅ [transactions] Record for ${checkoutRequestID} updated to: ${finalTransactionStatus}`);

        await errorsCollection.doc(`STK_PAYMENT_FAILED_${checkoutRequestID}`).set({
            type: 'STK_PAYMENT_ERROR',
            error: `STK Payment failed or was cancelled. ResultCode: ${resultCode}, ResultDesc: ${callback.Body.stkCallback.ResultDesc}`,
            checkoutRequestID: checkoutRequestID,
            callbackData: callback,
            createdAt: now,
        });
    }

    // Update the initial 'sales' request document to reflect the payment status
    // It will be further updated by the fulfillment logic once airtime is sent.
    await initialSalesRequestDocRef.update({
        mpesaPaymentStatus: finalTransactionStatus, // Store M-Pesa payment outcome
        mpesaReceiptNumber: mpesaReceiptNumber,
        mpesaTransactionDate: transactionDateFromMpesa,
        mpesaPhoneNumberUsed: phoneNumberUsedForPayment,
        mpesaAmountPaid: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
        fullStkCallback: callback, // Store full callback in the sales/request doc
        lastUpdated: now,
        reconciliationNeededAtPayment: needsReconciliation, // Reflect if payment stage needs recon
        // Importantly, fulfillment-related fields (bonus, total_sent, airtimeResult, providerUsed)
        // should NOT be set here, as they will be set by the unified fulfillment logic.
    });
    logger.info(`✅ [sales/initial_request] M-Pesa payment status for ${checkoutRequestID} updated.`);

    // If analytics notification failed and payment was successful, mark for reconciliation
    if (needsReconciliation && resultCode === 0) {
        await failedReconciliationsCollection.doc(checkoutRequestID).set({
            transactionId: checkoutRequestID,
            mpesaReceiptNumber: mpesaReceiptNumber,
            originalAmountPaid: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : 0,
            topupNumber: topupNumber,
            carrier: carrier,
            failureReason: 'ANALYTICS_NOTIFICATION_FAILED_AT_PAYMENT',
            errorDetails: 'Analytics server could not be notified for float deduction after successful payment.',
            mpesaCallbackDetails: callback,
            timestamp: now,
            status: 'PENDING_ANALYTICS_RECONCILIATION',
        }, { merge: true });
        logger.warn(`⚠️ Added ${checkoutRequestID} to 'failed_reconciliations' due to analytics notification failure.`);
    }

    res.json({ ResultCode: 0, ResultDesc: 'Callback received and payment status recorded by DaimaPay server.' });
});

// --- Start the Server ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    logger.info(`🚀 Server running on port ${PORT}`);
});