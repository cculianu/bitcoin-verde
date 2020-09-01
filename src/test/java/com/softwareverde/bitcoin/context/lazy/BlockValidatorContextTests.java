package com.softwareverde.bitcoin.context.lazy;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.test.BlockData;
import com.softwareverde.bitcoin.test.IntegrationTest;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.HexUtil;
import org.junit.Assert;
import org.junit.Test;

public class BlockValidatorContextTests extends IntegrationTest {
    protected void insertPreviousBlockHeader(final BlockchainSegmentId blockchainSegmentId, final Long blockHeight, final String blockHeaderData, final String previousBlockHashString, final String chainWorkString) throws Exception {
        final BlockHeaderInflater blockHeaderInflater = _masterInflater.getBlockHeaderInflater();
        final Sha256Hash previousBlockHash = Sha256Hash.fromHexString(previousBlockHashString);
        final BlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString(blockHeaderData));

        final ChainWork chainWork = ChainWork.fromHexString(chainWorkString);
        super.insertBlockHeader(blockchainSegmentId, blockHeader, blockHeight, previousBlockHash, chainWork);
    }

    @Test
    public void should_return_correct_MedianBlockTime() throws Exception {
        final BlockHeaderInflater blockHeaderInflater = _masterInflater.getBlockHeaderInflater();

        final BlockchainSegmentId blockchainSegmentId = BlockchainSegmentId.wrap(1L);

        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            synchronized (BlockHeaderDatabaseManager.MUTEX) {
                blockHeaderDatabaseManager.storeBlockHeader(blockHeaderInflater.fromBytes(HexUtil.hexStringToByteArray(BlockData.MainChain.GENESIS_BLOCK)));
            }
        }

        this.insertPreviousBlockHeader(blockchainSegmentId, 478570L, "000000207FA5085533AE45DF25A045DC1BFAFEF59E7D022987076D0000000000000000002E4A4054E64C3F5810C23EC0144D9793AAB2D5A7D77D1660EEE24D3D55E8B715B543815935470118DA1C00E0", "0000000000000000006D078729027D9EF5FEFA1BDC45A025DF45AE335508A57F", "00000000000000000000000000000000000000000074513A178FC01E1FB98D3A");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478571L, "000000200B6620D77A15ECB46A120430E27F3E6306D86D628C9FE200000000000000000073152AF68778A98FD984A158AEB29D28E094E23C3A7DFF02260C345791E52498C3FB8159354701188D5ABED9", "000000000000000000E29F8C626DD806633E7FE23004126AB4EC157AD720660B", "0000000000000000000000000000000000000000007452026191D8D47157C5C8");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478572L, "00000020DBC3EB18F4D57451085CAC98BC5EA228FCEEC617EF012401000000000000000022606E744A29F9D4A67FF1FCD2F0E31300DDBD145F8F1DB8A68270BFBDE77DD88FFF8159354701186AF175F8", "0000000000000000012401EF17C6EEFC28A25EBC98AC5C085174D5F418EBC3DB", "0000000000000000000000000000000000000000007452CAAB93F18AC2F5FE56");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478573L, "00000020774F568005A00BF4FFED76B52E3A7E5E5140A5371834A00000000000000000009F5DB27969FECC0EF71503279069B2DF981BA545592A7B425F353B5060E77F3E7E13825935470118DA70378E", "000000000000000000A0341837A540515E7E3A2EB576EDFFF40BA00580564F77", "000000000000000000000000000000000000000000745392F5960A41149436E4");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478574L, "0000002044068FFD5ACE2999D5E326A56CA17DF7ECBB4C89C218A80000000000000000002F0D316B08350F5CD998C6A11762D10ADB9F951B5F79CE2A073F8187C05F561F1B1C8259354701184834C623", "000000000000000000A818C2894CBBECF77DA16CA526E3D59929CE5AFD8F0644", "00000000000000000000000000000000000000000074545B3F9822F766326F72");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478575L, "000000205841AEA6F06F4A0E3560EA076CF1DF217EBDE943A92C16010000000000000000CF8FC3BAD8DAD139A3DD6A30481D87E1F760122573168002CC9EF7A58FC53AD387848259354701188A3B54F7", "000000000000000001162CA943E9BD7E21DFF16C07EA60350E4A6FF0A6AE4158", "000000000000000000000000000000000000000000745523899A3BADB7D0A800");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478576L, "00000020D22E87EAA7C68D9E8F947BF5DF3CABE9294050180BF4130000000000000000000EAE92D9B46D81A011A79726A802D4EB195A7AF8B70A09B0E115C391968C50D51C8A825935470118CD786D13", "00000000000000000013F40B18504029E9AB3CDFF57B948F9E8DC6A7EA872ED2", "0000000000000000000000000000000000000000007455EBD39C5464096EE08E");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478577L, "00000020FA4F8E791184C0CEE158961A0AC6F4299898F872F06A410100000000000000003EC6D34403E8B74BFE9711CE053468EFB269D87422B18DB202C3FA6CB7E503754598825902990118BE67E71E", "000000000000000001416AF072F8989829F4C60A1A9658E1CEC08411798E4FFA", "00000000000000000000000000000000000000000074568C0EEA477DC3346267");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478578L, "000000203BAE86EFF4775A05ACB8F089EF9F9DC9A123019AE19B630000000000000000008D262180063A2B9A841774DCEB255AD62CD285714B4B1DD5FA4D3172CC588269489F825942FF01186CA0E289", "000000000000000000639BE19A0123A1C99D9FEF89F0B8AC055A77F4EF86AE3B", "00000000000000000000000000000000000000000074570C3E7BEE8AC1069367");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478579L, "000000200BD6698AC0E5485B1569BE08C4B2E4850FDBA4875E146B01000000000000000045CB87C2183B186EBE02EE912DF1769A1E3795A49A8B22C6B5E3D6AA3E15990075A58259127F021876C195E8", "0000000000000000016B145E87A4DB0F85E4B2C408BE69155B48E5C08A69D60B", "000000000000000000000000000000000000000000745772CB04FE4FD1824E36");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478580L, "00000020A26F0E8CC7202DD2C70A3ABFED66A0DB44C929CA931B32020000000000000000A5651F4C4504F533CF44D9E6193EDC0B721D6D0660B0D7388B02FD765BF9C70E11AA8259D61E0318A83E2D93", "000000000000000002321B93CA29C944DBA066EDBF3A0AC7D22D20C78C0E6FA2", "0000000000000000000000000000000000000000007457C4D51963594B705D49");
        this.insertPreviousBlockHeader(blockchainSegmentId, 478581L, "00000020F3C6E921BB46DE2EBE0923C6FEF9433510F57E15543D1301000000000000000071CCD70BC227084B74BDA5634BE313C63C213F46AB8801411CB78809F8408C3D81C182598BE6031894141AA2", "000000000000000001133D54157EF5103543F9FEC62309BE2EDE46BB21E9C6F3", "00000000000000000000000000000000000000000074580676CBB7878AC4EA22");

        final MedianBlockTime expectedMedianBlockTime = MedianBlockTime.fromSeconds(1501727260L);

        final MedianBlockTime medianBlockTime;
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final LazyBlockValidatorContext blockValidatorContext = new LazyBlockValidatorContext(_masterInflater, blockchainSegmentId, null, null, databaseManager, null);
            medianBlockTime = blockValidatorContext.getMedianBlockTime(478581L);
        }

        Assert.assertEquals(expectedMedianBlockTime, medianBlockTime);
    }
}
