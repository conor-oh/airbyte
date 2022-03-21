/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.split_secrets;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Adds secrets to a partial config.
 */
public interface SecretsHydrator {

  /**
   * Adds secrets to a partial config.
   *
   * @param partialConfig partial config (without secrets)
   * @return full config with secrets
   */
  JsonNode hydrate(final JsonNode partialConfig);

  /**
   * Takes in the secret coordinate in form of a JSON and fetches the secret from the store without
   * any special transformations
   *
   * @param secretCoordinate The co-ordinate of the secret in the store in JSON format
   * @return original secret value
   */
  JsonNode simpleHydrate(final JsonNode secretCoordinate);

}
