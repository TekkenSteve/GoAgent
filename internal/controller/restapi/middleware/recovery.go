package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
	fiberRecover "github.com/gofiber/fiber/v2/middleware/recover"
)

func buildPanicMessage(ctx *fiber.Ctx, err any) string {
	return fmt.Sprintf("%s - %s %s PANIC DETECTED: %v\n%s\n",
		ctx.IP(), ctx.Method(), ctx.OriginalURL(), err, debug.Stack())
}

func logPanic(l logger.Interface) func(c *fiber.Ctx, err any) {
	return func(ctx *fiber.Ctx, err any) {
		l.Error(buildPanicMessage(ctx, err))
	}
}

// Recovery returns a Fiber middleware that recovers panics and logs them, with stack traces, via the given logger.
func Recovery(l logger.Interface) func(c *fiber.Ctx) error {
	return fiberRecover.New(fiberRecover.Config{
		EnableStackTrace:  true,
		StackTraceHandler: logPanic(l),
	})
}
