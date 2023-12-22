/**
 * @Author:      leafney
 * @GitHub:      https://github.com/leafney
 * @Project:     rose-amqp
 * @Date:        2023-12-22 12:04
 * @Description:
 */

package ramqp

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {

	t.Logf(FormatDuration(80 * time.Second))

}
