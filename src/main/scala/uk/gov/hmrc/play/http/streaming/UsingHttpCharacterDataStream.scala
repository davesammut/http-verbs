/*
 * Copyright 2015 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.play.http.streaming

import uk.gov.hmrc.play.audit.http.HeaderCarrier
import uk.gov.hmrc.play.http.logging.RequestId

import scala.collection.mutable.HashMap

trait UsingHttpCharacterDataStream[T] {

  private[streaming] def sourceData(resourceLocation: String): Iterator[Char] = scala.io.Source.fromURL(resourceLocation).iter
  implicit def convert(data: String) : T

  def processEntitiesWith(deliminator: Char, resourceLocation: String)(f: T => Unit)(implicit hc: HeaderCarrier): Unit = {
    val resourceData = sourceData(resourceLocation)
    Iterator.continually(resourceData.foreach(processCharacter(deliminator, f))).takeWhile(_ => resourceData.hasNext).toList
  }

  private[streaming] def processCharacter(deliminator: Char, entityProcessor: T => Unit)(characterFromStream: Char)(implicit hc: HeaderCarrier): Unit = {
    entityLocator(deliminator)(characterFromStream) match {
      case Some(entityFound) => entityProcessor(entityFound.toString())
      case None => //do nothing
    }
  }

  private val entityCharacterAccumulator = new HashMap[RequestId, StringBuilder]

  private def entityLocator(deliminator: Char)(characterFromStream: Char)(implicit hc: HeaderCarrier): Option[StringBuilder] = {
    val requestId = hc.requestId.getOrElse(throw new RuntimeException("Unable to process file. RequestId is missing!"))
    if (characterFromStream == deliminator) {
      entityCharacterAccumulator.remove(requestId)
    } else {
      entityCharacterAccumulator.get(requestId) match {
        case Some(sb) => entityCharacterAccumulator put(requestId, sb.append(characterFromStream.toString))
        case None => entityCharacterAccumulator put(requestId, new StringBuilder(characterFromStream.toString))
      }
      None
    }
  }
}
