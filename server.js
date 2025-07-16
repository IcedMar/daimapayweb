// server.js - Corrected STK Push Initiation and M-Pesa STK Callback Handling

// --- IMPORTS AND CONFIGURATION (no changes needed here) ---
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const admin = require('firebase-admin');
const { FieldValue } = require('firebase-admin/firestore');
const rateLimit = require('express-rate-limit');
const winston = require('winston');
const cors = require('cors');

// Initialize Firebase Admin SDK
const serviceAccount = JSON.parse(
    Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_JSON, 'base64').toString('utf-8')
);

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const firestore = admin.firestore();

// Firestore Collection References
const transactionsCollection = firestore.collection('transactions');
const salesCollection = firestore.collection('sales');
const errorsCollection = firestore.collection('errors');
const failedReconciliationsCollection = firestore.collection('failed_reconciliations');
const reconciledTransactionsCollection = firestore.collection('reconciled_transactions');
const airtimeBonusesCollection = firestore.collection('airtime_bonuses');
const carrierFloatsCollection = firestore.collection('carrier_floats');

// M-Pesa API Credentials from .env
const CONSUMER_KEY = process.env.CONSUMER_KEY;
const CONSUMER_SECRET = process.env.CONSUMER_SECRET;
const SHORTCODE = process.env.BUSINESS_SHORT_CODE;
const PASSKEY = process.env.PASSKEY;
const STK_CALLBACK_URL = process.env.CALLBACK_URL;
const ANALYTICS_SERVER_URL = process.env.ANALYTICS_SERVER_URL;

// Logger setup (no changes needed here)
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console(),
    ],
});

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// --- CORS Configuration (no changes needed here) ---
const allowedOrigins = [
    'https://daima-pay-portal.onrender.com',
    'https://dpanalyticsserver.onrender.com'
];
app.use(cors({
    origin: function (origin, callback) {
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            const msg = 'The CORS policy for this site does not allow access from the specified Origin.';
            return callback(new Error(msg), false);
        }
        return callback(null, true);
    },
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
    optionsSuccessStatus: 204
}));


// --- HELPER FUNCTIONS (no changes needed here) ---

async function getAccessToken() {
    const auth = Buffer.from(`${CONSUMER_KEY}:${CONSUMER_SECRET}`).toString('base64');
    try {
        const response = await axios.get('https://api.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials', {
            headers: { 'Authorization': `Basic ${auth}` }
        });
        return response.data.access_token;
    } catch (error) {
        logger.error('Error getting access token:', error.message);
        throw new Error('Failed to get M-Pesa access token.');
    }
}

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

function generatePassword(shortcode, passkey, timestamp) {
    const str = shortcode + passkey + timestamp;
    return Buffer.from(str).toString('base64');
}

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

async function updateCarrierFloatBalance(floatName, amount) {
    logger.info(`Attempting to update float balance for ${floatName} by ${amount}`);
    try {
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

// --- RATE LIMITING (add trust proxy here for express-rate-limit warning) ---
app.set('trust proxy', 1); // <--- ADD THIS LINE FOR EXPRESS-RATE-LIMIT WARNING

const stkPushLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 20,
    message: 'Too many STK Push requests from this IP, please try again after a minute.',
    statusCode: 429,
    headers: true,
});

const stkCallbackRateLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 100,
    message: 'Too many STK Callback requests, please try again later.',
    statusCode: 429,
    headers: true,
});


// --- ENDPOINTS ---

// 1. STK Push Initiation Endpoint
app.post('/stk-push', stkPushLimiter, async (req, res) => {
    const { amount, phoneNumber, recipient } = req.body;

    if (!amount || !phoneNumber || !recipient) {
        return res.status(400).json({ success: false, message: 'Missing required parameters: amount, phoneNumber, recipient.' });
    }

    const timestamp = generateTimestamp();
    const password = generatePassword(SHORTCODE, PASSKEY, timestamp);

    // *** IMPORTANT CHANGE HERE ***
    // We will save a preliminary document first, and then get M-Pesa's CheckoutRequestID
    // to use as the true document ID for tracking the transaction.
    let mpesaCheckoutRequestID = null; // Will store the CheckoutRequestID from Safaricom

    logger.info(`Initiating STK Push for recipient: ${recipient}, amount: ${amount}, customer: ${phoneNumber}`);

    try {
        const accessToken = await getAccessToken();
        const detectedCarrier = detectCarrier(recipient);

        const stkPushResponse = await axios.post('https://api.safaricom.co.ke/mpesa/stkpush/v1/processrequest', {
            BusinessShortCode: SHORTCODE,
            Password: password,
            Timestamp: timestamp,
            TransactionType: 'CustomerPayBillOnline',
            Amount: amount,
            PartyA: phoneNumber,
            PartyB: SHORTCODE,
            PhoneNumber: phoneNumber,
            CallBackURL: STK_CALLBACK_URL,
            AccountReference: recipient,
            TransactionDesc: `Airtime for ${recipient}`
        }, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });

        logger.info('STK Push Request Sent:', stkPushResponse.data);

        // *** Use M-Pesa's CheckoutRequestID as the primary identifier ***
        if (stkPushResponse.data && stkPushResponse.data.ResponseCode === '0' && stkPushResponse.data.CheckoutRequestID) {
            mpesaCheckoutRequestID = stkPushResponse.data.CheckoutRequestID;

            // --- Create or Update the sales document using M-Pesa's CheckoutRequestID ---
            // This document will store all stages of the transaction (initiation, callback, fulfillment)
            await salesCollection.doc(mpesaCheckoutRequestID).set({
                saleId: mpesaCheckoutRequestID, // M-Pesa's CheckoutRequestID as our primary ID
                initiatorPhoneNumber: phoneNumber,
                recipient: recipient,
                amount: parseFloat(amount),
                carrier: detectedCarrier,
                mpesaPaymentStatus: 'PENDING_MPESA_CONFIRMATION', // Initial status
                createdAt: FieldValue.serverTimestamp(),
                lastUpdated: FieldValue.serverTimestamp(),
                type: 'STK_PUSH_REQUEST',
                mpesaResponse: stkPushResponse.data, // Store full M-Pesa STK Push response
                // Other fulfillment-related fields will be added by the callback and fulfillment logic
            });
            logger.info(`✅ Sale request document ${mpesaCheckoutRequestID} created/updated with STK Push response.`);
        } else {
            // Handle cases where STK Push was not successful or CheckoutRequestID is missing
            logger.error('❌ STK Push initiation failed or CheckoutRequestID missing in response:', stkPushResponse.data);
            const errorMessage = stkPushResponse.data.CustomerMessage || stkPushResponse.data.ResponseDescription || 'STK Push initiation failed';
            await errorsCollection.add({
                type: 'STK_PUSH_INITIATION_FAILED_NO_CHECKOUT_ID',
                error: errorMessage,
                response: stkPushResponse.data,
                requestBody: req.body,
                createdAt: FieldValue.serverTimestamp(),
            });
            return res.status(500).json({ success: false, message: 'Failed to initiate STK Push or get M-Pesa ID.', error: errorMessage });
        }

        res.status(200).json({ success: true, message: 'STK Push initiated successfully.', data: { CheckoutRequestID: mpesaCheckoutRequestID, ...stkPushResponse.data } });

    } catch (error) {
        logger.error('❌ Error during STK Push initiation:', {
            message: error.message,
            stack: error.stack,
            requestBody: req.body,
            errorResponse: error.response ? error.response.data : 'No response data'
        });

        const errorMessage = error.response ? error.response.data : error.message;

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

    // Ensure the callback structure is as expected
    if (!callback || !callback.Body || !callback.Body.stkCallback) {
        logger.error('❌ Invalid STK Callback structure received:', JSON.stringify(callback));
        await errorsCollection.add({
            type: 'INVALID_STK_CALLBACK_STRUCTURE',
            callbackData: callback,
            createdAt: now,
        });
        return res.json({ ResultCode: 1, ResultDesc: 'Invalid callback structure.' });
    }

    const resultCode = callback.Body.stkCallback.ResultCode;
    const checkoutRequestID = callback.Body.stkCallback.CheckoutRequestID; // This is the M-Pesa CheckoutRequestID

    if (!checkoutRequestID) {
        logger.error('❌ STK Callback received without CheckoutRequestID:', JSON.stringify(callback));
        await errorsCollection.add({
            type: 'STK_CALLBACK_MISSING_CHECKOUT_ID',
            callbackData: callback,
            createdAt: now,
        });
        return res.json({ ResultCode: 1, ResultDesc: 'Missing CheckoutRequestID in callback.' });
    }


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

    // Retrieve the initial sales request document using the M-Pesa CheckoutRequestID
    const initialSalesRequestDocRef = salesCollection.doc(checkoutRequestID); // <--- NOW THIS WILL MATCH!
    const initialSalesRequestDoc = await initialSalesRequestDocRef.get();

    if (!initialSalesRequestDoc.exists) {
        logger.error('❌ No matching initial sales request for CheckoutRequestID in Firestore:', checkoutRequestID);
        // This should now only happen if the STK Push initiation failed before saving to Firestore
        // or if there's a serious latency issue / data loss.
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
    const topupNumber = initialRequestData.recipient;
    const carrier = initialRequestData.carrier;

    let finalTransactionStatus;
    let needsReconciliation = false;

    const transactionDocRef = transactionsCollection.doc(checkoutRequestID);

    if (resultCode === 0) {
        logger.info(`✅ M-Pesa payment successful for ${checkoutRequestID}. Marking for fulfillment.`);
        finalTransactionStatus = 'RECEIVED_PENDING_FULFILLMENT';

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
            status: finalTransactionStatus,
            fulfillmentStatus: 'PENDING', // Initial fulfillment status
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            createdAt: initialRequestData.createdAt || now,
            lastUpdated: now,
            relatedSaleId: checkoutRequestID,
            reconciliationNeeded: false,
        }, { merge: true });
        logger.info(`✅ [transactions] Record for ${checkoutRequestID} created/updated with status: ${finalTransactionStatus}`);

        // --- Notify Analytics Server for float deduction (customer's payment received) ---
        try {
            logger.info(`Attempting to notify Analytics Server for payment ${checkoutRequestID} (type: PAYMENT_RECEIVED)...`);
            const analyticsPayload = {
                transactionId: checkoutRequestID,
                amount: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : 0,
                status: 'PAYMENT_RECEIVED',
                carrier: carrier,
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
            needsReconciliation = true;
            await transactionDocRef.update({
                reconciliationNeeded: true,
                errorMessage: `Analytics notification for payment receipt failed: ${deductionError.message}`
            });
            logger.warn(`⚠️ Transaction ${checkoutRequestID} marked for reconciliation due to analytics notification failure.`);
        }

    } else {
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
            status: finalTransactionStatus,
            fulfillmentStatus: 'NOT_APPLICABLE',
            mpesaResultCode: resultCode,
            mpesaResultDesc: callback.Body.stkCallback.ResultDesc,
            errorMessage: `M-Pesa payment failed: ${callback.Body.stkCallback.ResultDesc}`,
            createdAt: initialRequestData.createdAt || now,
            lastUpdated: now,
            relatedSaleId: checkoutRequestID,
            reconciliationNeeded: false,
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
    await initialSalesRequestDocRef.update({
        mpesaPaymentStatus: finalTransactionStatus,
        mpesaReceiptNumber: mpesaReceiptNumber,
        mpesaTransactionDate: transactionDateFromMpesa,
        mpesaPhoneNumberUsed: phoneNumberUsedForPayment,
        mpesaAmountPaid: amountPaidByCustomer ? parseFloat(amountPaidByCustomer) : null,
        fullStkCallback: callback,
        lastUpdated: now,
        reconciliationNeededAtPayment: needsReconciliation,
    });
    logger.info(`✅ [sales/initial_request] M-Pesa payment status for ${checkoutRequestID} updated.`);

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
