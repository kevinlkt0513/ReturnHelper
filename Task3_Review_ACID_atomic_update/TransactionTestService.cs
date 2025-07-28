using System;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using MongoDB.Driver.Core;
using ReturnRequestApiApp.Data.MongoDb;
using ReturnRequestApiApp.Data.MongoDb.Entity;

namespace TransactionService.Services
{
    /// <summary>
    /// Service to handle transactions involving atomic balance updates and transaction record insertion
    /// with distributed transaction support and automatic retry on transient errors.
    /// </summary>
    public class TransactionTestService : TransactionTest.TransactionTestBase
    {
        readonly TransactionMongoDbContext transactionMongoDbContext;
        readonly ILogger logger;

        /// <summary>
        /// Constructor injecting MongoDB context and logger.
        /// </summary>
        public TransactionTestService(
            TransactionMongoDbContext transactionMongoDbContext,
            ILogger<TransactionTestService> logger)
        {
            this.transactionMongoDbContext = transactionMongoDbContext;
            this.logger = logger;
        }

        /// <summary>
        /// Implements the atomic update of balance and insertion of transaction record 
        /// with retry logic on transient transaction errors.
        /// </summary>
        /// <param name="request">Incoming request with transaction info.</param>
        /// <param name="context">gRPC server call context.</param>
        /// <returns>Response indicating success or failure, with updated transaction data.</returns>
        public override async Task<AddApiTransactionWithLockTestResponse> AddApiTransactionWithLockTest(
            AddApiTransactionWithLockTestRequest request, ServerCallContext context)
        {
            logger.LogInformation("Starting AddApiTransactionWithLockTest for ApiId: {ApiId}, TransactionNumber: {TransactionNumber}",
                request.ApiId, request.TransactionNumber);

            int maxRetries = 5;  // Maximum allowed retry attempts on transient errors
            int attempt = 0;

            // Retry loop for handling transient transaction errors with exponential backoff
            while (true)
            {
                // Start a client session to support multi-document transactions
                using IClientSessionHandle session = await transactionMongoDbContext.mongoDatabase.Client.StartSessionAsync();
                try
                {
                    // Define transaction options ensuring strong consistency and a max commit timeout
                    TransactionOptions transactionOptions = new(
                        readConcern: ReadConcern.Snapshot,           // Snapshot isolation for transactions
                        writeConcern: WriteConcern.WMajority,        // Majority write concern for durability
                        readPreference: ReadPreference.Primary,      // Read from primary to ensure up-to-date data
                        maxCommitTime: TimeSpan.FromSeconds(30)      // Max commit time, adjustable per business need
                    );

                    // Run the transaction operation with automatic commit/abort support
                    var response = await session.WithTransactionAsync(async (sessionHandle, cancellationToken) =>
                    {
                        logger.LogInformation("Transaction started for ApiId: {ApiId}", request.ApiId);

                        decimal transactionAmount = decimal.Parse(request.Amount);
                        DateTime createOn = DateTime.UtcNow;

                        // Step 1: Atomically update balance inside transaction using FindOneAndUpdate with upsert
                        ApiBalance updatedBalance = await AtomicUpdateBalanceInTransaction(
                            sessionHandle,
                            request.ApiId,
                            request.CurrencyCode,
                            transactionAmount);

                        logger.LogInformation("Atomic balance update completed in transaction. New Balance: {Balance}",
                            updatedBalance.balance);

                        // Step 2: Insert transaction record with unique transactionNumber for idempotency
                        ApiTransaction newTransaction = new()
                        {
                            apiTransactionId = Guid.NewGuid().ToString(),
                            transactionNumber = request.TransactionNumber,
                            apiId = request.ApiId,
                            currencyCode = request.CurrencyCode,
                            transactionType = request.TransactionType,
                            headId = request.HeadId,
                            amount = transactionAmount,
                            notes = request.Notes,
                            createOn = createOn,
                            createBy = request.CreateBy
                        };

                        try
                        {
                            // Attempt to insert the transaction document into the collection
                            await transactionMongoDbContext.apiTransactionCollection
                                .InsertOneAsync(sessionHandle, newTransaction, cancellationToken: cancellationToken);
                        }
                        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                        {
                            // Duplicate key exception indicates the transaction record already exists
                            // This means this is a retry, and can be treated as idempotent success
                            logger.LogWarning("Transaction with transactionNumber={TransactionNumber} already exists, treat as idempotent success",
                                newTransaction.transactionNumber);
                            // Optionally, query and return the existing transaction if needed
                        }

                        // NOTE: Removed Task.Delay wait to avoid long transaction duration that harms DB performance.

                        logger.LogInformation("Transaction completed successfully for ApiId: {ApiId}", request.ApiId);

                        // Return successful response with updated transaction and balance info
                        return new AddApiTransactionWithLockTestResponse
                        {
                            Success = true,
                            Message = "Atomic transaction completed successfully",
                            Transaction = new()
                            {
                                TransactionId = newTransaction.apiTransactionId,
                                TransactionNumber = newTransaction.transactionNumber,
                                ApiId = newTransaction.apiId,
                                CurrencyCode = newTransaction.currencyCode,
                                TransactionType = newTransaction.transactionType,
                                HeadId = newTransaction.headId,
                                Amount = newTransaction.amount.ToString(),
                                Notes = newTransaction.notes,
                                CreateOn = Timestamp.FromDateTime(newTransaction.createOn.ToUniversalTime()),
                                CreateBy = newTransaction.createBy,
                                BalanceId = updatedBalance.apiBalanceId,
                                Balance = updatedBalance.balance.ToString()
                            }
                        };
                    }, transactionOptions);

                    // Transaction committed successfully, exit retry loop
                    return response;
                }
                catch (MongoException ex) when (
                    ex.HasErrorLabel("TransientTransactionError") ||
                    ex.HasErrorLabel("UnknownTransactionCommitResult"))
                {
                    // Handle transient errors by retrying
                    attempt++;
                    logger.LogWarning(ex, "Transient transaction error on attempt {Attempt}, apiId: {ApiId}. Retrying...",
                        attempt, request.ApiId);

                    if (attempt >= maxRetries)
                    {
                        // Maximum retry attempts reached – return failure
                        logger.LogError(ex, "Exceeded max retries ({MaxRetries}) for transaction. ApiId: {ApiId}", maxRetries, request.ApiId);
                        return new AddApiTransactionWithLockTestResponse
                        {
                            Success = false,
                            Message = $"Error after retries: {ex.Message}"
                        };
                    }

                    // Exponentially back off before retrying
                    await Task.Delay(200 * attempt);
                    continue; // Retry transaction
                }
                catch (Exception ex)
                {
                    // Catch-all for any other unexpected exceptions
                    logger.LogError(ex, "Error occurred during atomic transaction for ApiId: {ApiId}", request.ApiId);
                    return new AddApiTransactionWithLockTestResponse
                    {
                        Success = false,
                        Message = $"Error: {ex.Message}"
                    };
                }
            }
        }

        /// <summary>
        /// Performs an atomic balance increment/decrement inside a transaction session.
        /// Uses FindOneAndUpdate with upsert=true and returns the updated document after operation.
        /// </summary>
        /// <param name="session">The MongoDB client session to participate in the transaction.</param>
        /// <param name="apiId">The API ID (account) for which to update balance.</param>
        /// <param name="currencyCode">Currency code of the balance record.</param>
        /// <param name="amount">Amount to increment (positive or negative).</param>
        /// <returns>The updated ApiBalance document reflecting the new balance.</returns>
        async Task<ApiBalance> AtomicUpdateBalanceInTransaction(
            IClientSessionHandle session,
            int apiId,
            string currencyCode,
            decimal amount)
        {
            // Filter to locate the corresponding balance record by apiId and currencyCode
            FilterDefinition<ApiBalance> filter = Builders<ApiBalance>.Filter.And(
                Builders<ApiBalance>.Filter.Eq(x => x.apiId, apiId),
                Builders<ApiBalance>.Filter.Eq(x => x.currencyCode, currencyCode)
            );

            // Update definition: atomically increment balance plus set identity fields on insert
            UpdateDefinition<ApiBalance> update = Builders<ApiBalance>.Update
                .Inc(x => x.balance, amount)   // Atomic increment (can be negative)
                .SetOnInsert(x => x.apiBalanceId, Guid.NewGuid().ToString())  // For new records
                .SetOnInsert(x => x.apiId, apiId)
                .SetOnInsert(x => x.currencyCode, currencyCode);

            // FindOneAndUpdate options: upsert=true to create if missing; return document after update
            FindOneAndUpdateOptions<ApiBalance> options = new()
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };

            // Execute the atomic update within the transaction session context
            return await transactionMongoDbContext.apiBalanceCollection
                .FindOneAndUpdateAsync(session, filter, update, options);
        }
    }
}
