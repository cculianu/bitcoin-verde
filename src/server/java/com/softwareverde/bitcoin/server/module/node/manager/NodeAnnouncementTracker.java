package com.softwareverde.bitcoin.server.module.node.manager;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockRelationship;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManagerFactory;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.CircleBuffer;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

public class NodeAnnouncementTracker {
    static final int MAXIMUM_HEADERS = 500;
    static final int PROMOTION_THRESHOLD = 10;

    public NodeAnnouncementTracker(NodePromotedCallback nodePromotedCallback) {
        _nodePromotedCallback = nodePromotedCallback;
    }

    public interface NodePromotedCallback { void onNodePromoted(BitcoinNode bitcoinNode); }

    protected static class HeaderAnnouncement {
        final BlockHeader _blockHeader;
        final WeakReference<BitcoinNode> _bitcoinNode;

        HeaderAnnouncement(final BlockHeader blockHeader, final WeakReference<BitcoinNode> bitcoinNode) {
            _blockHeader = blockHeader;
            _bitcoinNode = bitcoinNode;
        }

        public BlockHeader getBlockHeader() {
            return _blockHeader;
        }

        public WeakReference<BitcoinNode> getBitcoinNode() {
            return _bitcoinNode;
        }
    }

    final CircleBuffer<HeaderAnnouncement> _headerAnnouncements = new CircleBuffer<>(MAXIMUM_HEADERS);
    final WeakHashMap<BitcoinNode, Integer> _nodeScores = new WeakHashMap<>();
    final NodePromotedCallback _nodePromotedCallback;

    public synchronized void onNewHeaderReceived(final FullNodeDatabaseManagerFactory fullNodeDatabaseManagerFactory, final BitcoinNode bitcoinNode, final BlockHeader blockHeader) {
        try (final FullNodeDatabaseManager fullNodeDatabaseManager = fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final BlockchainDatabaseManager blockchainDatabaseManager = fullNodeDatabaseManager.getBlockchainDatabaseManager();
            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = fullNodeDatabaseManager.getBlockHeaderDatabaseManager();
            final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(blockHeader.getHash());
            final boolean isBlockConnectedToChain = blockHeaderDatabaseManager.isBlockConnectedToChain(blockId, headBlockchainSegmentId, BlockRelationship.ANY);

            if (! isBlockConnectedToChain) { return; }
        }
        catch (final DatabaseException exception) {
            Logger.warn(exception);
        }

        final HeaderAnnouncement headerAnnouncement = new HeaderAnnouncement(blockHeader, new WeakReference<>(bitcoinNode));
        _headerAnnouncements.push(headerAnnouncement);
    }

    public synchronized void onNewValidatedBlock(final Block block) {
        final Sha256Hash blockHash = block.getHash();
        for (final HeaderAnnouncement headerAnnouncement : _headerAnnouncements) {
            if (headerAnnouncement.getBlockHeader().getHash().equals(blockHash)) {
                _rewardBitcoinNodeOnValidatedBlock(headerAnnouncement);
                return;
            }
        }
    }

    private void _rewardBitcoinNodeOnValidatedBlock(final HeaderAnnouncement headerAnnouncement) {
        final BitcoinNode bitcoinNode = headerAnnouncement.getBitcoinNode().get();
        if (bitcoinNode != null) {
            final Integer currentNodeScore = _nodeScores.getOrDefault(bitcoinNode, 0);
            final int updatedNodeScore = currentNodeScore + 1;

            if (updatedNodeScore == PROMOTION_THRESHOLD) {
                _nodePromotedCallback.onNodePromoted(bitcoinNode);
                _nodeScores.remove(bitcoinNode);
                return;
            }

            _nodeScores.put(bitcoinNode, updatedNodeScore);
        }
    }
}
