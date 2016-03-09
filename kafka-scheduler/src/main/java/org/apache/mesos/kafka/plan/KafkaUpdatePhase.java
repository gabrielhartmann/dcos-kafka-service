package org.apache.mesos.kafka.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.mesos.kafka.config.KafkaConfigService;
import org.apache.mesos.kafka.offer.KafkaOfferRequirementProvider;
import org.apache.mesos.kafka.scheduler.KafkaScheduler;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Phase;

public class KafkaUpdatePhase implements Phase {
  private List<Block> blocks;
  private String configName;
  private KafkaConfigService config;
  private UUID id;

  public KafkaUpdatePhase(
      String configName,
      KafkaOfferRequirementProvider offerReqProvider) {

    this.configName = configName;
    this.config = KafkaScheduler.getConfigState().fetch(configName);
    this.blocks = createBlocks(configName, offerReqProvider);
    this.id = UUID.randomUUID();
  }

  @Override
  public List<Block> getBlocks() {
    return blocks;
  }

  @Override
  public Block getBlock(UUID id) {
    for (Block block : getBlocks()) {
      if (block.getId().equals(id)) {
        return block;
      }
    }

    return null;
  }

  @Override
  public Block getBlock(int index){
    return getBlocks().get(index);
  }


  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public String getName() {
    return "Update to: " + configName;
  }

  @Override
  public boolean isComplete() {
    for (Block block : blocks) {
      if (!block.isComplete()) {
        return false;
      }
    }

    return true;
  }

  private List<Block> createBlocks(
      String configName,
      KafkaOfferRequirementProvider offerReqProvider) {

    List<Block> blocks = new ArrayList<Block>();

    for (int i=0; i<config.getBrokerCount(); i++) {
      blocks.add(new KafkaUpdateBlock(offerReqProvider, configName, i));
    }

    return blocks;
  }
}
