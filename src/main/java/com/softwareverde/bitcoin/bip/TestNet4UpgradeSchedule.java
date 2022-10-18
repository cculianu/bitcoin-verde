package com.softwareverde.bitcoin.bip;

import com.softwareverde.bitcoin.chain.time.MedianBlockTime;

public class TestNet4UpgradeSchedule implements UpgradeSchedule {
    // NOTE: BCH-specific activation heights are usually one greater than the BCHN activation heights
    //       since BCHN uses the previous block height for activations.

    private static final long BIP16_ACTIVATION_BLOCK_HEIGHT = 2L;
    private static final long BIP34_ACTIVATION_BLOCK_HEIGHT = 3L;
    private static final long BIP65_ACTIVATION_BLOCK_HEIGHT = 4L;
    private static final long BIP66_ACTIVATION_BLOCK_HEIGHT = 5L;
    private static final long BIP68_ACTIVATION_BLOCK_HEIGHT = 6L;
    private static final long BIP112_ACTIVATION_BLOCK_HEIGHT = 6L;
    private static final long BIP113_ACTIVATION_BLOCK_HEIGHT = 6L;
    private static final long BUIP55_ACTIVATION_BLOCK_HEIGHT = 7L; // a.k.a. UAHF
    private static final long HF20171113_ACTIVATION_BLOCK_HEIGHT = 3001L;
    private static final long HF20181115_ACTIVATION_BLOCK_HEIGHT = 4001L;

    private static final long HF20190515_ACTIVATION_TIME = 0L;
    private static final long HF20191115_ACTIVATION_TIME = 1599487350L;
    private static final long HF20200515_ACTIVATION_TIME = 0L;
    private static final long HF20201115_ACTIVATION_TIME = 1605441600L;
    private static final long HF20220515_ACTIVATION_TIME = 1637694000L;
    private static final long HF20230515_ACTIVATION_TIME = 1684152000L;

    @Override
    public Boolean isMinimalNumberEncodingRequired(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20190515_ACTIVATION_TIME);
    }

    @Override
    public Boolean isBitcoinCashSignatureHashTypeEnabled(final Long blockHeight) {
        return (blockHeight >= BUIP55_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areOnlyPushOperationsAllowedWithinUnlockingScript(final Long blockHeight) {
        return (blockHeight >= HF20181115_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isPayToScriptHashEnabled(final Long blockHeight) {
        return (blockHeight >= BIP16_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isAsertDifficultyAdjustmentAlgorithmEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20201115_ACTIVATION_TIME);
    }

    @Override
    public Boolean isCw144DifficultyAdjustmentAlgorithmEnabled(final Long blockHeight) {
        return (blockHeight >= HF20171113_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isEmergencyDifficultyAdjustmentAlgorithmEnabled(final Long blockHeight) {
        return (blockHeight >= BUIP55_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isBlockHeightWithinCoinbaseRequired(final Long blockHeight) {
        return (blockHeight >= BIP34_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isRelativeLockTimeEnabled(final Long blockHeight) {
        return (blockHeight >= BIP68_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean shouldUseMedianBlockTimeForTransactionLockTime(final Long blockHeight) {
        return (blockHeight >= BIP113_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean shouldUseMedianBlockTimeForBlockTimestamp(final Long blockHeight) {
        return (blockHeight >= BIP113_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean isCheckLockTimeOperationEnabled(final Long blockHeight) {
        return blockHeight >= BIP65_ACTIVATION_BLOCK_HEIGHT;
    }

    @Override
    public Boolean isCheckSequenceNumberOperationEnabled(final Long blockHeight) {
        return (blockHeight >= BIP112_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areAllInvalidSignaturesRequiredToBeEmpty(final Long blockHeight) {
        return (blockHeight >= HF20171113_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areSignaturesRequiredToBeStrictlyEncoded(final Long blockHeight) {
        return (blockHeight >= BUIP55_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areCanonicalSignatureEncodingsRequired(final Long blockHeight) {
        return (blockHeight >= HF20171113_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean arePublicKeysRequiredToBeStrictlyEncoded(final Long blockHeight) {
        return (blockHeight >= BUIP55_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areDerSignaturesRequiredToBeStrictlyEncoded(final Long blockHeight) {
        return (blockHeight >= BIP66_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areUnusedValuesAfterScriptExecutionDisallowed(final Long blockHeight) {
        return (blockHeight >= HF20181115_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areTransactionsLessThanOneHundredBytesDisallowed(final Long blockHeight) {
        return (blockHeight >= HF20181115_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areTransactionsLessThanSixtyFiveBytesDisallowed(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20230515_ACTIVATION_TIME);
    }

    @Override
    public Boolean areUnusedValuesAfterSegwitScriptExecutionAllowed(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20190515_ACTIVATION_TIME);
    }

    @Override
    public Boolean isSignatureOperationCountingVersionTwoEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20200515_ACTIVATION_TIME);
    }

    @Override
    public Boolean isCheckDataSignatureOperationEnabled(final Long blockHeight) {
        return (blockHeight >= HF20181115_ACTIVATION_BLOCK_HEIGHT);
    }

    @Override
    public Boolean areSchnorrSignaturesEnabledWithinMultiSignature(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20191115_ACTIVATION_TIME);
    }

    @Override
    public Boolean isReverseBytesOperationEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20200515_ACTIVATION_TIME);
    }

    @Override
    public Boolean areIntrospectionOperationsEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20220515_ACTIVATION_TIME);
    }

    @Override
    public Boolean are64BitScriptIntegersEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20220515_ACTIVATION_TIME);
    }

    @Override
    public Boolean isMultiplyOperationEnabled(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20220515_ACTIVATION_TIME);
    }

    @Override
    public Boolean areTransactionVersionsRestricted(final MedianBlockTime medianBlockTime) {
        return (medianBlockTime.getCurrentTimeInSeconds() >= HF20230515_ACTIVATION_TIME);
    }
}
